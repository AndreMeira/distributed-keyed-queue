package homelab.keyedqueue.infrastructure.redis.script


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.service.lock.LockStore.Hold
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection
import io.lettuce.core.ScriptOutputType
import zio.*

import java.time.Instant


/**
 * Take a named lock if free, reclaiming an expired lease inline — `lua/lock_acquire.lua`.
 *
 * Named, so it checks the one lock's lease directly and needs no sweep: a dead holder is released here, by
 * whoever next wants the lock.
 *
 * @param ref the digest this script was loaded under, from [[LockScripts]]
 */
final class LockAcquireScript(ref: LuaScript.Sha):

  /** Multi, because the reply is `{token, leaseUntil}` or nil. */
  private val output: ScriptOutputType = ScriptOutputType.MULTI

  /**
   * Take the lock, or report it held.
   *
   * @param name the lock to take
   * @param ttl how long the resulting hold survives without a refresh
   * @return the hold, or `None` when it is held under a live lease; aborts with `RedisFailure` when the
   *         store fails or the reply cannot be read
   */
  def run(name: String, ttl: Duration): ZIO[Connection.Commands, RedisFailure, Option[Hold]] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.evalsha[Any](ref, output, LockKeys.core, args(name, ttl)*))
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(read(name)(reply)))

  /**
   * The lock's name, then the lease length.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives
   * @return `name`, `ttl`, in the order `lua/lock_acquire.lua` reads them
   */
  private def args(name: String, ttl: Duration): Array[Array[Byte]] =
    Array(LuaScript.utf8(name), LuaScript.utf8(ttl.toMillis.toString))

  /**
   * Read `{token, leaseUntil}`, or nil when the lock is held.
   *
   * @param name the lock this reply is for
   * @param value the raw reply
   * @return the hold, or `None`; `MalformedReply` when the reply is neither
   */
  private def read(name: String)(value: Any): Either[RedisFailure, Option[Hold]] =
    LuaScript.Decode
      .sized(2) {
        for
          token <- LuaScript.Decode.long.at(0)
          until <- LuaScript.Decode.long.at(1)
        yield Hold(name, token, Instant.ofEpochMilli(until))
      }
      .orNone
      .decode("lock.acquire", value)


object LockAcquireScript:

  /**
   * Register `lua/lock_acquire.lua` and hold its digest.
   *
   * @return the script; aborts with `RedisFailure` when it is missing or rejected
   */
  def make: ZIO[Connection.Commands, RedisFailure, LockAcquireScript] =
    LuaScript.register("lua/lock_acquire.lua").map(LockAcquireScript(_))
