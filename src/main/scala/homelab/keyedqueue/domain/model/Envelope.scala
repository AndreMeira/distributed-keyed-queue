package homelab.keyedqueue.domain.model


import homelab.keyedqueue.domain.types.*
import zio.Chunk

import java.time.Instant


/**
 * The unit that travels and is stored: routing metadata the queue reads, and cargo it does not.
 *
 * Mirrors the wire message field for field, because the transformer between them should contain no
 * decisions — a field that needs logic to cross that boundary is a sign the two have drifted apart.
 *
 * The queue name is deliberately absent: it is the address a message was sent to, not a property of the
 * message (`docs/research/redis-keyed-queue.md`).
 *
 * @param key what ordering is defined by: one key is worked by one consumer at a time
 * @param messageId idempotency and correlation in logs
 * @param payloadType the payload's schema identity — a stable name and version, never a class name
 * @param encoding how `payload` is serialised
 * @param sentAt the sender's clock: good for lag metrics, not for decisions
 * @param payload the cargo, which the queue never parses
 */
final case class Envelope(
  key: MessageKey,
  messageId: String,
  payloadType: String,
  encoding: Encoding,
  sentAt: Option[Instant],
  payload: Chunk[Byte],
)
