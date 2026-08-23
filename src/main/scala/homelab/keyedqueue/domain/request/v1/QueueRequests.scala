package homelab.keyedqueue.domain.request.v1


import homelab.keyedqueue.domain.model.Envelope
import homelab.keyedqueue.domain.types.*
import zio.{ Chunk, Duration }


/**
 * What a caller asks of the queue.
 *
 * Each mirrors its wire message field for field — same names, same shapes, richer types — so the
 * transformer between them carries no decisions. Where one of these differs from the proto, the difference
 * is the point: the queue name is separate from the envelope because it is an address, the enums have no
 * `Unspecified` because an unusable request is refused at the boundary rather than carried inwards.
 */
object QueueRequests


/**
 * Accept a message for a key.
 *
 * @param queue the queue to append to
 * @param envelope the message
 */
final case class EnqueueRequest(queue: QueueName, envelope: Envelope)


/**
 * Wait for a message.
 *
 * @param queue the queue to take from
 * @param maxWait how long the caller is prepared to wait; the service clamps it to its own ceiling
 */
final case class DequeueRequest(queue: QueueName, maxWait: Duration)


/**
 * Report what happened to a claimed message.
 *
 * @param receipt the handle from the delivery
 * @param outcome what the consumer decided
 * @param retryAfter how long to hold the key back before retrying; ignored for `Done`
 */
final case class SettleRequest(receipt: Receipt, outcome: Verdict, retryAfter: Duration)


/**
 * Renew everything a consumer still holds.
 *
 * @param receipts the handles it believes it holds; empty is legal and means "still here, holding nothing"
 */
final case class HeartbeatRequest(receipts: Chunk[Receipt])
