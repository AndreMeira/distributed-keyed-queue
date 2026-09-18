package homelab.keyedqueue.domain.model.queue


import homelab.keyedqueue.domain.types.*
import zio.Chunk

import java.time.Instant


/**
 * The unit that travels and is stored: routing metadata the queue reads, and cargo it does not.
 *
 * The queue name is deliberately absent: it is the address a message was sent to, not a property of the
 * message (`docs/research/connection-keyed-queue.md`).
 *
 * @param key what ordering is defined by: one key is worked by one consumer at a time
 * @param messageId this message's own name: unique among those queued for its key, and what a settle or
 *                  a repeated enqueue is matched on
 * @param payloadType the payload's schema identity — a stable name and version, never a class name
 * @param encoding how `payload` is serialised, as a media type the queue never reads
 * @param sentAt the sender's clock: good for lag metrics, not for decisions
 * @param payload the cargo, which the queue never parses
 */
final case class Message(
  key: MessageKey,
  messageId: MessageId,
  payloadType: String,
  encoding: String,
  sentAt: Option[Instant],
  payload: Chunk[Byte],
)
