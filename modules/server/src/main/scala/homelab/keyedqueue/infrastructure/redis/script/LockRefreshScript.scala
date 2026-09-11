package homelab.keyedqueue.infrastructure.redis.script


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection
import io.lettuce.core.ScriptOutputType
import zio.*

import java.time.Instant


/**
 * Push a held lock's lease forward, if the caller still holds it — `lua/lock/refresh.lua`.
 *
 * Token-only: a holder whose lease lapsed but whom nobody displaced is late, not lost, and may extend.
 *
 * @param ref the digest this script was loaded under, from [[LockScripts]]
 */
final class LockRefreshScript(ref: LuaScript.Sha):

  /** Multi, because the reply is `{leaseUntil, ok}`. */
  private val output: ScriptOutputType = ScriptOutputType.MULTI

  /**
   * Extend the lock's lease.
   *
   * @param name the lock to keep alive
   * @param token the fence token the hold was granted under
   * @param ttl how much longer to grant
   * @return the new deadline and whether the hold survived; aborts with `RedisFailure` when the store fails
   *         or the reply cannot be read
   */
  def run(name: LockName, token: Token, ttl: Duration): ZIO[Connection.Commands, RedisFailure, (Instant, Boolean)] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.evalsha[Any](ref, output, LockKeys.refresh, args(name, token, ttl)*))
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(read(reply)))

  /**
   * The lock's name, the token, then the lease length.
   *
   * @param name the lock to keep alive
   * @param token the fence token
   * @param ttl how much longer to grant
   * @return `name`, `token`, `ttl`, in the order `lua/lock/refresh.lua` reads them
   */
  private def args(name: LockName, token: Token, ttl: Duration): Array[Array[Byte]] =
    Array(LuaScript.utf8(name), LuaScript.utf8(token.toString), LuaScript.utf8(ttl.toMillis.toString))

  /**
   * Read `{leaseUntil, ok}`.
   *
   * @param value the raw reply
   * @return the new deadline and whether the hold survived; `MalformedReply` when the reply cannot be read
   */
  private def read(value: Any): Either[RedisFailure, (Instant, Boolean)] =
    LuaScript.Decode
      .sized(2) {
        for
          until <- LuaScript.Decode.long.at(0)
          ok    <- LuaScript.Decode.long.at(1)
        yield (Instant.ofEpochMilli(until), ok == 1L)
      }
      .decode("lock.refresh", value)


object LockRefreshScript:

  /**
   * Register `lua/lock/refresh.lua` and hold its digest.
   *
   * @return the script; aborts with `RedisFailure` when it is missing or rejected
   */
  def make: ZIO[Connection.Commands, RedisFailure, LockRefreshScript] =
    LuaScript.register("lua/lock/refresh.lua").map(LockRefreshScript(_))
