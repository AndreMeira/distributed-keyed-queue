package homelab.keyedqueue.domain.request.v1


import homelab.keyedqueue.domain.model.Message.Encoding
import homelab.keyedqueue.domain.types.{ MessageId, MessageKey }
import zio.Chunk

import java.time.Instant


/**
 * Accept a message for a key.
 *
 * Mirrors its wire message field for field — same names, same shapes, richer types — so the transformer
 * between them carries no decisions. Where it differs from the proto, the difference is the point: the
 * queue name is separate from the message because it is an address.
 *
 * @param queue the queue to append to, as it arrived
 * @param message the message, as it arrived
 */
final case class EnqueueRequest(queue: String, message: EnqueueRequest.Message)


object EnqueueRequest:

  /**
   * A message as the wire can state it.
   *
   * The twin of [[homelab.keyedqueue.domain.model.Message]], and deliberately not it: the domain message
   * carries a [[MessageKey]] and a [[MessageId]], which are claims that someone checked. Here they are the
   * strings a caller sent, and the parse is what turns them into names the store can address.
   *
   * `encoding` and the cargo cross unchanged. The first is already total — the codec refuses the wire's
   * `UNSPECIFIED` — and the second is bytes this service never reads.
   *
   * @param key what ordering is to be defined by
   * @param messageId what this message is to be addressed by
   * @param payloadType the payload's schema identity
   * @param encoding how `payload` is serialised
   * @param sentAt the sender's clock
   * @param payload the cargo, which the queue never parses
   */
  final case class Message(
    key: String,
    messageId: String,
    payloadType: String,
    encoding: Encoding,
    sentAt: Option[Instant],
    payload: Chunk[Byte],
  )
