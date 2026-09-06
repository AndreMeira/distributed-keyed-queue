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
 * message (`docs/research/connection-keyed-queue.md`).
 *
 * @param key what ordering is defined by: one key is worked by one consumer at a time
 * @param messageId this message's own name: unique among those queued for its key, and what a settle or
 *                  a repeated enqueue is matched on
 * @param payloadType the payload's schema identity — a stable name and version, never a class name
 * @param encoding how `payload` is serialised
 * @param sentAt the sender's clock: good for lag metrics, not for decisions
 * @param payload the cargo, which the queue never parses
 */
final case class Message(
  key: MessageKey,
  messageId: MessageId,
  payloadType: String,
  encoding: Message.Encoding,
  sentAt: Option[Instant],
  payload: Chunk[Byte],
)


object Message:

  /**
   * How a payload is serialised.
   *
   * There is no `Unspecified` here, unlike on the wire: a message that does not say how to read it cannot
   * be acted on, so it is refused at the boundary rather than carried inwards as a state every later match
   * has to remember to reject.
   */
  enum Encoding:
    case Json, Protobuf
