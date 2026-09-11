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
 * Enter for a named lock: granted at once when free with nobody queued, a tail ticket otherwise —
 * `lua/lock/acquire.lua`.
 *
 * The ticket is what makes the lock fair: grants follow ticket order among tickets still within their
 * patience, and a newcomer goes to the tail. Reclaims an expired lease inline, and prunes expired tickets
 * from the head, so neither a dead holder nor a dead waiter needs a background pass to get out of the way.
 *
 * @param ref the digest this script was loaded under, from [[LockScripts]]
 */
final class LockAcquireScript(ref: LuaScript.Sha):

  /** Multi, because the reply is `{granted, …}` in either shape. */
  private val output: ScriptOutputType = ScriptOutputType.MULTI

  /**
   * Take the lock, or queue for it.
   *
   * @param name the lock to take
   * @param ttl how long the resulting hold survives without a refresh
   * @param patience how long the caller will wait — the ticket's lifetime, measured on the store's clock
   * @return the grant, or the ticket to wait on; aborts with `RedisFailure` when the store fails or the
   *         reply cannot be read
   */
  def run(name: LockName, ttl: Duration, patience: Duration): ZIO[Connection.Commands, RedisFailure, LockAcquireScript.Reply] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.evalsha[Any](ref, output, LockKeys.granting(name), args(name, ttl, patience)*))
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(read(name)(reply)))

  /**
   * The lock's name, the lease length, then the ticket's lifetime.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives
   * @param patience how long the ticket lives
   * @return `name`, `ttl`, `patience`, in the order `lua/lock/acquire.lua` reads them
   */
  private def args(name: LockName, ttl: Duration, patience: Duration): Array[Array[Byte]] =
    Array(LuaScript.utf8(name), LuaScript.utf8(ttl.toMillis.toString), LuaScript.utf8(patience.toMillis.toString))

  /**
   * Read `{1, token, leaseUntil}` as a grant or `{0, ticketId, recheckMillis}` as a queued entry.
   *
   * @param name the lock this reply is for
   * @param value the raw reply
   * @return the reply; `MalformedReply` when it is neither shape
   */
  private def read(name: LockName)(value: Any): Either[RedisFailure, LockAcquireScript.Reply] =
    LuaScript.Decode
      .sized(3) {
        LuaScript.Decode.long.at(0).flatMap {
          case 1     =>
            for
              token <- LuaScript.Decode.long.at(1)
              until <- LuaScript.Decode.long.at(2)
            yield LockAcquireScript.Reply.Granted(Hold(LockClaim(name, Token(token)), Instant.ofEpochMilli(until)))
          case 0     =>
            for
              id      <- LuaScript.Decode.long.at(1)
              recheck <- LuaScript.Decode.long.at(2)
            yield LockAcquireScript.Reply.Queued(id, Duration.fromMillis(recheck))
          case other =>
            LuaScript.Decode.fail(RedisFailure.MalformedReply(s"lock.acquire answered with status $other"))
        }
      }
      .decode("lock.acquire", value)


object LockAcquireScript:

  /**
   * Register `lua/lock/acquire.lua` and hold its digest.
   *
   * @return the script; aborts with `RedisFailure` when it is missing or rejected
   */
  def make: ZIO[Connection.Commands, RedisFailure, LockAcquireScript] =
    LuaScript.register("lua/lock/acquire.lua").map(LockAcquireScript(_))

  /** What entering answered: the lock, or a place in its queue. */
  enum Reply:

    /** The lock was free with nobody queued, and is now held. */
    case Granted(hold: Hold)

    /**
     * Queued: the ticket to ask with, and how long until the answer can next change.
     *
     * @param ticket the ticket's identity
     * @param recheck the delay after which granting could answer differently
     */
    case Queued(ticket: Long, recheck: Duration)
