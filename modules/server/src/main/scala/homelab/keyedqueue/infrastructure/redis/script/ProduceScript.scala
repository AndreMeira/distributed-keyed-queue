package homelab.keyedqueue.infrastructure.redis.script


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.Message
import homelab.keyedqueue.infrastructure.codecs.storage.StoredMessage
import homelab.keyedqueue.infrastructure.redis.Connection.Commands
import homelab.keyedqueue.infrastructure.redis.{ Connection, Namespace }
import io.lettuce.core.ScriptOutputType
import zio.*


/**
 * Append a message and make its key claimable — `lua/produce.lua`.
 *
 * The conditional push inside the script is what keeps a key in `ready` at most once, which is why this is
 * one call and not a read followed by a write.
 *
 * The message is serialised here rather than by the caller: what a message looks like at rest is this
 * adapter's choice — see [[StoredMessage]].
 *
 * @param ref the digest this script was loaded under, from [[Scripts]]
 */
final class ProduceScript(ref: LuaScript.Sha):

  /** Integer, because the script's last act is an `LLEN`. */
  private val output: ScriptOutputType = ScriptOutputType.INTEGER

  /**
   * Append a message and make its key claimable.
   *
   * @param ns the queue to append in
   * @param message the message; the key it carries decides where it lands
   * @return the key's depth after the append; aborts with `QueueError` when the store fails or the reply
   *         cannot be read
   */
  def run(ns: Namespace, message: Message): ZIO[Connection.Commands, QueueError, Long] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.evalsha[Any](ref, output, keys(ns, message), args(message)*))
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(read(reply)))

  /**
   * The five structures an append touches: where claimable keys queue, where their state is kept, where
   * this key's own messages and payloads accumulate, and the doorbell it rings when the key becomes
   * claimable.
   *
   * @param ns the queue to append in
   * @param message the message; the key it carries decides where it lands
   * @return `ready`, `state`, `msgs`, `payloads`, `wake`, in the order `lua/produce.lua` reads them
   */
  private def keys(ns: Namespace, message: Message): Array[String] =
    Array(ns.ready, ns.state, ns.msgs(message.key), ns.payloads(message.key), ns.wake)

  /**
   * The key to append under, and the message as it will be stored.
   *
   * @param message the message to serialise
   * @return `key`, `id`, `payload`, in the order `lua/produce.lua` reads them
   */
  private def args(message: Message): Array[Array[Byte]] =
    Array(
      LuaScript.utf8(message.key),
      LuaScript.utf8(message.messageId),
      StoredMessage.toBytes(message).toArray,
    )

  /**
   * Read the key's depth after the append.
   *
   * @param value the raw reply
   * @return how many messages that key now holds, or `MalformedReply` when the reply is not an integer
   */
  private def read(value: Any): Either[QueueError, Long] =
    LuaScript.Decode.long.decode("produce", value)


object ProduceScript:

  /**
   * Register `lua/produce.lua` and hold the digest it was given.
   *
   * @return the script, ready to run; aborts with `QueueError` if it is missing or the server rejects it
   */
  def make: ZIO[Connection.Commands, QueueError, ProduceScript] =
    LuaScript.register("lua/produce.lua").map(ProduceScript(_))
