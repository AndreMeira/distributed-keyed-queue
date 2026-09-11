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
 * Take a named lock only if it is free now and nobody queued first — `lua/lock/try.lua`.
 *
 * Reclaims an expired lease inline, like the ticketed path. "Free now" includes free of waiters: a live
 * ticket means someone queued first, and granting past them would be barging.
 *
 * @param ref the digest this script was loaded under, from [[LockScripts]]
 */
final class LockTryScript(ref: LuaScript.Sha):

  /** Multi, because the reply is `{token, leaseUntil}` or nil. */
  private val output: ScriptOutputType = ScriptOutputType.MULTI

  /**
   * Take the lock, or report it taken or queued for.
   *
   * @param name the lock to take
   * @param ttl how long the resulting hold survives without a refresh
   * @return the hold, or `None` when it is held under a live lease or has live tickets; aborts with
   *         `RedisFailure` when the store fails or the reply cannot be read
   */
  def run(name: LockName, ttl: Duration): ZIO[Connection.Commands, RedisFailure, Option[Hold]] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.evalsha[Any](ref, output, LockKeys.granting(name), args(name, ttl)*))
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(read(name)(reply)))

  /**
   * The lock's name, then the lease length.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives
   * @return `name`, `ttl`, in the order `lua/lock/try.lua` reads them
   */
  private def args(name: LockName, ttl: Duration): Array[Array[Byte]] =
    Array(LuaScript.utf8(name), LuaScript.utf8(ttl.toMillis.toString))

  /**
   * Read `{token, leaseUntil}`, or nil when the lock is held or queued for.
   *
   * @param name the lock this reply is for
   * @param value the raw reply
   * @return the hold, or `None`; `MalformedReply` when the reply is neither
   */
  private def read(name: LockName)(value: Any): Either[RedisFailure, Option[Hold]] =
    LuaScript.Decode
      .sized(2) {
        for
          token <- LuaScript.Decode.long.at(0)
          until <- LuaScript.Decode.long.at(1)
        yield Hold(LockClaim(name, Token(token)), Instant.ofEpochMilli(until))
      }
      .orNone
      .decode("lock.try", value)


object LockTryScript:

  /**
   * Register `lua/lock/try.lua` and hold its digest.
   *
   * @return the script; aborts with `RedisFailure` when it is missing or rejected
   */
  def make: ZIO[Connection.Commands, RedisFailure, LockTryScript] =
    LuaScript.register("lua/lock/try.lua").map(LockTryScript(_))
