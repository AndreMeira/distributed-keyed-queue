package homelab.keyedqueue.domain.request.v1

import zio.Duration

/**
 * A caller's unchecked ask for work: which queue, how long it will wait, how much it will take.
 *
 * What it asks for is a preference, not a promise. Parsing clamps both amounts to what the service offers,
 * and refuses a patience of none.
 *
 * @param queue the queue to take from, as it arrived
 * @param maxWait how long the caller is prepared to wait; the parse clamps it to the service's ceiling
 * @param maxBatch the most messages to claim at once; clamped likewise
 */
final case class DequeueRequest(queue: String, maxWait: Duration, maxBatch: Int)
