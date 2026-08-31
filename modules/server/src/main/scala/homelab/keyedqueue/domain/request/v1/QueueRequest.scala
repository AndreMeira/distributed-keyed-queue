package homelab.keyedqueue.domain.request.v1


import homelab.keyedqueue.domain.types.*
import zio.{ Chunk, Duration }

import java.time.Instant


/**
 * What a caller asks of the queue.
 *
 * Each mirrors its wire message field for field — same names, same shapes, richer types — so the
 * transformer between them carries no decisions. Where one of these differs from the proto, the difference
 * is the point: the queue name is separate from the message because it is an address, the enums have no
 * `Unspecified` because an unusable request is refused at the boundary rather than carried inwards.
 *
 * Nested rather than five top-level types, so the umbrella is what carries that shared note and the names
 * do not have to repeat the word: `QueueRequest.Dequeue`, not `DequeueRequest`. Which also keeps them
 * distinguishable from their wire counterparts, whose names the generated code fixes.
 */
object QueueRequest:

  /**
   * Accept a message for a key.
   *
   * @param queue the queue to append to, as it arrived
   * @param message the message, as it arrived
   */
  final case class Enqueue(queue: String, message: Enqueue.Message)

  object Enqueue:

    /**
     * A message as the wire can state it.
     *
     * The twin of [[homelab.keyedqueue.domain.model.Message]], and deliberately not it: the domain message
     * carries a [[MessageKey]] and a [[MessageId]], which are claims that someone checked. Here they are
     * the strings a caller sent, and the parse is what turns them into names the store can address.
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

  /**
   * Wait for a message.
   *
   * @param queue the queue to take from, as it arrived
   * @param maxWait how long the caller is prepared to wait; the parse clamps it to the service's ceiling
   * @param maxBatch the most messages to claim at once; clamped likewise
   */
  final case class Dequeue(queue: String, maxWait: Duration, maxBatch: Int)

  /**
   * Report what happened to some of what a claim owns.
   *
   * '''Nothing here is a domain type, and that is the point.''' A `ClaimRef` or a `MessageId` in this
   * position would be evidence — of a receipt this service issued, of an id it can address — minted by a
   * codec for values nobody has looked at. Raw strings can make no such claim, so the only way to obtain
   * the evidence is to go through [[homelab.keyedqueue.domain.service.validation.QueueInputValidation]],
   * which is what turns this into a `Settlement`.
   *
   * `Verdict` is the exception, and is not evidence: the wire's `UNSPECIFIED` is refused by the codec, so
   * what arrives here is a total value rather than an unchecked claim.
   *
   * @param receipt the handle from the delivery, as it arrived
   * @param outcomes what became of each message named. What is not named stays owed, and the claim ends
   *                 once nothing is
   * @param retryAfter how long the key should wait before anyone works it again, asked for by a nack; zero
   *                   is what an absent duration decodes to
   */
  final case class Settle(
    receipt: String,
    outcomes: Chunk[MessageOutcome],
    retryAfter: Duration,
  )

  /**
   * Renew everything a consumer still holds.
   *
   * @param receipts the handles it believes it holds, as they arrived; empty is legal and means "still
   *                 here, holding nothing"
   */
  final case class Heartbeat(receipts: Chunk[String])

  /**
   * What a consumer did with one message of its batch.
   *
   * Keeps its full name: it is a part of a request rather than one of them, so there is no suffix to drop
   * and nothing to gain from calling it `Outcome`, which is already what the verdict enum answers.
   *
   * @param messageId which message, as the delivery named it — a name, not yet a [[MessageId]]
   * @param outcome what became of it
   */
  final case class MessageOutcome(messageId: String, outcome: Verdict)
