package homelab.keyedqueue.infrastructure.redis.script


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection
import io.lettuce.core.ScriptOutputType
import zio.*


/**
 * Withdraw a ticket — `lua/lock/abandon.lua`.
 *
 * Best effort: a waiter that dies without withdrawing is pruned at its deadline by the next pass over the
 * queue's head, so this only tidies eagerly what pruning would tidy anyway.
 *
 * @param ref the digest this script was loaded under, from [[LockScripts]]
 */
final class LockAbandonScript(ref: LuaScript.Sha):

  /** Integer, because the script answers whether it withdrew anything. */
  private val output: ScriptOutputType = ScriptOutputType.INTEGER

  /**
   * Withdraw the ticket.
   *
   * @param name the lock queued for
   * @param ticket the ticket's identity
   * @return true when withdrawn, false when it was already gone; aborts with `RedisFailure` when the store
   *         fails or the reply cannot be read
   */
  def run(name: LockName, ticket: Long): ZIO[Connection.Commands, RedisFailure, Boolean] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.evalsha[Any](ref, output, LockKeys.ticket(name), args(name, ticket)*))
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(read(reply)))

  /**
   * The lock's name, then the ticket.
   *
   * @param name the lock queued for
   * @param ticket the ticket's identity
   * @return `name`, `ticket`, in the order `lua/lock/abandon.lua` reads them
   */
  private def args(name: LockName, ticket: Long): Array[Array[Byte]] =
    Array(LuaScript.utf8(name), LuaScript.utf8(ticket.toString))

  /**
   * Read whether the withdrawal applied.
   *
   * @param value the raw reply
   * @return true when a ticket was removed; `MalformedReply` when the reply is not an integer
   */
  private def read(value: Any): Either[RedisFailure, Boolean] =
    LuaScript.Decode.long.map(_ == 1L).decode("lock.abandon", value)


object LockAbandonScript:

  /**
   * Register `lua/lock/abandon.lua` and hold its digest.
   *
   * @return the script; aborts with `RedisFailure` when it is missing or rejected
   */
  def make: ZIO[Connection.Commands, RedisFailure, LockAbandonScript] =
    LuaScript.register("lua/lock/abandon.lua").map(LockAbandonScript(_))
