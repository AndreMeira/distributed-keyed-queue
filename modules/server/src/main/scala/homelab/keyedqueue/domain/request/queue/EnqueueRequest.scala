package homelab.keyedqueue.domain.request.queue


import homelab.keyedqueue.domain.types.{ MessageId, MessageKey }
import zio.Chunk

import java.time.Instant


/**
 * A caller's ask to append a message to a key.
 *
 * @param queue the queue to append to, as it arrived
 * @param message the message, as it arrived
 */
final case class EnqueueRequest(queue: String, message: EnqueueRequest.Message)


object EnqueueRequest:

  /**
   * A message as a caller stated it, before anyone has checked it.
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
    encoding: String,
    sentAt: Option[Instant],
    payload: Chunk[Byte],
  )
