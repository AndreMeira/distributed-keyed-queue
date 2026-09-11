package homelab.keyedqueue.infrastructure.redis.script


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection
import io.lettuce.core.ScriptOutputType
import zio.*


/**
 * Remove holds abandoned past the grace window, waking any waiter on each — `lua/lock/trim.lua`.
 *
 * Hygiene, not liveness: a dead holder whose lock someone wants is reclaimed inline by their acquire; this
 * pass exists for the holds — and the waiter lists — nobody will ever ask for again, which no request can
 * reach.
 *
 * @param ref the digest this script was loaded under, from [[LockScripts]]
 */
final class LockTrimScript(ref: LuaScript.Sha):

  /** Multi, because the reply is the names removed. */
  private val output: ScriptOutputType = ScriptOutputType.MULTI

  /**
   * Remove holds expired for longer than the grace.
   *
   * @param grace how long past lease expiry a hold survives before it may be removed
   * @param limit the most holds one pass removes
   * @return the names removed, oldest lease first; aborts with `RedisFailure` when the store fails or the
   *         reply cannot be read
   */
  def run(grace: Duration, limit: Int): ZIO[Connection.Commands, RedisFailure, Chunk[LockName]] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.evalsha[Any](ref, output, LockKeys.trim, args(grace, limit)*))
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(read(reply)))

  /**
   * The grace, the batch bound, then the waiters-list prefix the script builds keys from.
   *
   * @param grace how long past expiry a hold survives
   * @param limit the most holds one pass removes
   * @return `grace`, `limit`, `prefix`, in the order `lua/lock/trim.lua` reads them
   */
  private def args(grace: Duration, limit: Int): Array[Array[Byte]] =
    Array(
      LuaScript.utf8(grace.toMillis.toString),
      LuaScript.utf8(limit.toString),
      LuaScript.utf8(LockKeys.waitersPrefix),
    )

  /**
   * Read the names removed.
   *
   * @param value the raw reply
   * @return the names; `MalformedReply` when the reply is not an array of strings
   */
  private def read(value: Any): Either[RedisFailure, Chunk[LockName]] =
    LuaScript.Decode.text.each.map(_.map(LockName(_))).decode("lock.trim", value)


object LockTrimScript:

  /**
   * Register `lua/lock/trim.lua` and hold its digest.
   *
   * @return the script; aborts with `RedisFailure` when it is missing or rejected
   */
  def make: ZIO[Connection.Commands, RedisFailure, LockTrimScript] =
    LuaScript.register("lua/lock/trim.lua").map(LockTrimScript(_))
