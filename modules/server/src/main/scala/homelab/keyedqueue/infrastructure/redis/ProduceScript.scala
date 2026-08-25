package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.Message
import homelab.keyedqueue.infrastructure.codecs.storage.StoredMessage
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
 * @param sha the digest this script was loaded under
 * @param ns the queue to append in
 * @param message the message; the key it carries decides where it lands
 */
final case class ProduceScript(sha: String, ns: Namespace, message: Message) extends LuaScript[Long]:

  /**
   * Integer, because the script's last act is an `LLEN`.
   *
   * @return `INTEGER`
   */
  override def output: ScriptOutputType = ScriptOutputType.INTEGER

  /**
   * The three lists and hashes an append touches: where claimable keys queue, where their state is kept,
   * and where this key's own messages accumulate.
   *
   * @return `ready`, `state`, `msgs`, in the order `lua/produce.lua` reads them
   */
  override def keys: Array[String] = Array(ns.ready, ns.state, ns.msgs(message.key))

  /**
   * The key to append under, and the message as it will be stored.
   *
   * @return `key`, `payload`, in the order `lua/produce.lua` reads them
   */
  override def args: Array[Array[Byte]] =
    Array(LuaScript.utf8(message.key), StoredMessage.toBytes(message).toArray)

  /**
   * Read the key's depth after the append.
   *
   * @param value the raw reply
   * @return how many messages that key now holds; aborts with `MalformedReply` when the reply is not an
   *         integer
   */
  override def read(value: Any): IO[QueueError, Long] =
    ZIO.fromEither(LuaScript.number("produce")(value))
