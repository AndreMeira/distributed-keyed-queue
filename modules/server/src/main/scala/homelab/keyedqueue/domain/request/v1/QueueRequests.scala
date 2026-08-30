package homelab.keyedqueue.domain.request.v1


import homelab.keyedqueue.domain.model.Message
import homelab.keyedqueue.domain.types.*
import zio.{ Chunk, Duration }


/**
 * What a caller asks of the queue.
 *
 * Each mirrors its wire message field for field — same names, same shapes, richer types — so the
 * transformer between them carries no decisions. Where one of these differs from the proto, the difference
 * is the point: the queue name is separate from the message because it is an address, the enums have no
 * `Unspecified` because an unusable request is refused at the boundary rather than carried inwards.
 */
object QueueRequests

/**
 * Accept a message for a key.
 *
 * @param queue the queue to append to
 * @param message the message
 */
final case class EnqueueRequest(queue: QueueName, message: Message)

/**
 * Wait for a message.
 *
 * @param queue the queue to take from
 * @param maxWait how long the caller is prepared to wait; the service clamps it to its own ceiling
 * @param maxBatch the most messages to claim at once; clamped likewise
 */
final case class DequeueRequest(queue: QueueName, maxWait: Duration, maxBatch: Int)


/**
 * Report what happened to a claimed message.
 *
 * @param receipt the handle from the delivery
 * @param outcome what the consumer decided
 * @param retryAfter how long to hold the key back before retrying; ignored for `Done`
 * @param outcomes what became of each message named. What is not named stays owed, and the claim ends
 *                 once nothing is
 * @param retryAfter how long the key should wait before anyone works it again, asked for by a nack
 */
final case class SettleRequest(
  receipt: ClaimRef,
  outcomes: Chunk[MessageOutcome],
  retryAfter: Duration,
)


/**
 * What a consumer did with one message of its batch.
 *
 * @param messageId which message, as the delivery named it
 * @param outcome what became of it
 */
final case class MessageOutcome(messageId: MessageId, outcome: Verdict)

/**
 * Renew everything a consumer still holds.
 *
 * @param receipts the handles it believes it holds; empty is legal and means "still here, holding nothing"
 */
final case class HeartbeatRequest(receipts: Chunk[ClaimRef])
