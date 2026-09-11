package homelab.keyedqueue.infrastructure.redis.script


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.LockClaim
import homelab.keyedqueue.domain.service.lock.LockStore.Hold
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection
import io.lettuce.core.ScriptOutputType
import zio.*

import java.time.Instant


/**
 * One ticketed waiter asking for its turn — `lua/lock/grant.lua`.
 *
 * Every waiter runs this off the same broadcast wake; only the head ticket can win, so the race is a
 * check. A refusal carries the delay after which the answer can change, so the waiter parks until a known
 * event rather than on a poll.
 *
 * @param ref the digest this script was loaded under, from [[LockScripts]]
 */
final class LockGrantScript(ref: LuaScript.Sha):

  /** Multi, because the reply is `{granted, …}` in any of three shapes. */
  private val output: ScriptOutputType = ScriptOutputType.MULTI

  /**
   * Ask for the lock, by ticket.
   *
   * @param name the lock queued for
   * @param ticket the ticket from the enter
   * @param ttl how long the resulting hold survives without a refresh
   * @return the grant, the next delay, or that the ticket is gone; aborts with `RedisFailure` when the
   *         store fails or the reply cannot be read
   */
  def run(name: LockName, ticket: Long, ttl: Duration): ZIO[Connection.Commands, RedisFailure, LockGrantScript.Reply] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.evalsha[Any](ref, output, LockKeys.granting(name), args(name, ticket, ttl)*))
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(read(name)(reply)))

  /**
   * The lock's name, the ticket, then the lease length.
   *
   * @param name the lock queued for
   * @param ticket the ticket's identity
   * @param ttl how long the hold survives
   * @return `name`, `ticket`, `ttl`, in the order `lua/lock/grant.lua` reads them
   */
  private def args(name: LockName, ticket: Long, ttl: Duration): Array[Array[Byte]] =
    Array(LuaScript.utf8(name), LuaScript.utf8(ticket.toString), LuaScript.utf8(ttl.toMillis.toString))

  /**
   * Read `{1, token, leaseUntil}`, `{0, recheckMillis}`, or `{2, 0}`.
   *
   * @param name the lock this reply is for
   * @param value the raw reply
   * @return the reply; `MalformedReply` when it is none of the three
   */
  private def read(name: LockName)(value: Any): Either[RedisFailure, LockGrantScript.Reply] =
    LuaScript.Decode.long
      .at(0)
      .flatMap {
        case 1     =>
          for
            token <- LuaScript.Decode.long.at(1)
            until <- LuaScript.Decode.long.at(2)
          yield LockGrantScript.Reply.Granted(Hold(LockClaim(name, Token(token)), Instant.ofEpochMilli(until)))
        case 0     => LuaScript.Decode.long.at(1).map(millis => LockGrantScript.Reply.Wait(Duration.fromMillis(millis)))
        case 2     => LuaScript.Decode.succeed(LockGrantScript.Reply.Gone)
        case other => LuaScript.Decode.fail(RedisFailure.MalformedReply(s"lock.grant answered with status $other"))
      }
      .decode("lock.grant", value)


object LockGrantScript:

  /**
   * Register `lua/lock/grant.lua` and hold its digest.
   *
   * @return the script; aborts with `RedisFailure` when it is missing or rejected
   */
  def make: ZIO[Connection.Commands, RedisFailure, LockGrantScript] =
    LuaScript.register("lua/lock/grant.lua").map(LockGrantScript(_))

  /** What asking answered: the lock, a wait, or a queue that no longer knows the ticket. */
  enum Reply:

    /** The ticket was head and the lock free: it is now held. */
    case Granted(hold: Hold)

    /**
     * Not yet: held, or queued behind the head.
     *
     * @param recheck the delay after which the answer can change
     */
    case Wait(recheck: Duration)

    /** The ticket is not in the queue — expired and pruned, or withdrawn. */
    case Gone
