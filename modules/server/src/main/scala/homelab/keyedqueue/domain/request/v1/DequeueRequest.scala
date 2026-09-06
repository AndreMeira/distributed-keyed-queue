package homelab.keyedqueue.domain.request.v1

import zio.Duration

/**
 * Wait for a message.
 *
 * Mirrors its wire message field for field, so the transformer between them carries no decisions. What a
 * caller asks for is a preference rather than a promise: the parse clamps both amounts to what the service
 * offers, and refuses a patience of none.
 *
 * @param queue the queue to take from, as it arrived
 * @param maxWait how long the caller is prepared to wait; the parse clamps it to the service's ceiling
 * @param maxBatch the most messages to claim at once; clamped likewise
 */
final case class DequeueRequest(queue: String, maxWait: Duration, maxBatch: Int)
