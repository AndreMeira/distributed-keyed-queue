package homelab.keyedqueue.infrastructure.redis.script


import homelab.common.error.ApplicationError
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection
import io.lettuce.core.ScriptOutputType
import zio.*


/**
 * Release a lock, if the caller still holds it — `lua/lock_release.lua`.
 *
 * The fence is checked and advanced, so a released token cannot act again and a release racing a reclaim
 * loses cleanly.
 *
 * @param ref the digest this script was loaded under, from [[LockScripts]]
 */
final class LockReleaseScript(ref: LuaScript.Sha):

  /** Integer, because the script answers whether it applied. */
  private val output: ScriptOutputType = ScriptOutputType.INTEGER

  /**
   * Release the lock.
   *
   * @param name the lock to release
   * @param token the fence token the hold was granted under
   * @return true when released, false when the token is stale; aborts with `RedisFailure` when the store
   *         fails or the reply cannot be read
   */
  def run(name: String, token: Long): ZIO[Connection.Commands, RedisFailure, Boolean] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.evalsha[Any](ref, output, LockKeys.all, args(name, token)*))
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(LuaScript.Decode.long.map(_ == 1L).decode("lock.release", reply)))

  /**
   * The lock's name, then the token that authorises releasing it.
   *
   * @param name the lock to release
   * @param token the fence token
   * @return `name`, `token`, in the order `lua/lock_release.lua` reads them
   */
  private def args(name: String, token: Long): Array[Array[Byte]] =
    Array(LuaScript.utf8(name), LuaScript.utf8(token.toString))


object LockReleaseScript:

  /**
   * Register `lua/lock_release.lua` and hold its digest.
   *
   * @return the script; aborts with `RedisFailure` when it is missing or rejected
   */
  def make: ZIO[Connection.Commands, RedisFailure, LockReleaseScript] =
    LuaScript.register("lua/lock_release.lua").map(LockReleaseScript(_))
