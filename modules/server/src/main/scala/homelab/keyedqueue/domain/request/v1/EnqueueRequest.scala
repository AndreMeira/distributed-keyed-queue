package homelab.keyedqueue.domain.request.v1


import homelab.keyedqueue.domain.model.Message.Encoding
import homelab.keyedqueue.domain.types.{ MessageId, MessageKey }
import zio.Chunk

import java.time.Instant


/**
 * A caller's unchecked ask to append a message to a key.
 *
 * The queue is separate from the message because it is the address a message was sent to, not a property
 * of the message.
 *
 * @param queue the queue to append to, as it arrived
 * @param message the message, as it arrived
 */
final case class EnqueueRequest(queue: String, message: EnqueueRequest.Message)


object EnqueueRequest:

  /**
   * A message as a caller stated it, before anyone has checked it.
   *
   * The twin of [[homelab.keyedqueue.domain.model.Message]], and deliberately not it: the domain message
   * carries a [[MessageKey]] and a [[MessageId]], which assert that someone checked. Here they are the
   * strings a caller sent, and parsing is what turns them into names the store can address.
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
