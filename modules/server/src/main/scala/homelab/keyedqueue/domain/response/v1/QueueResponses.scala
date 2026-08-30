package homelab.keyedqueue.domain.response.v1


import homelab.keyedqueue.domain.model.{ Delivery, Message }
import homelab.keyedqueue.domain.types.*
import zio.Chunk

import java.time.Instant


/**
 * What the queue answers.
 *
 * As with the requests, these mirror their wire messages so the transformer stays derivable. Note what is
 * *not* an error here: an empty `delivery` and a `Stale` outcome are ordinary results of an at-least-once
 * queue, carried where a caller has to look at them.
 */
object QueueResponses

/**
 * A message was accepted.
 *
 * @param keyDepth how many messages its key now has queued — a metric, not a decision
 */
final case class EnqueueResponse(keyDepth: Long)


/**
 * The outcome of a wait: a claim over some of one key's messages, or nothing.
 *
 * @param receipt the claim every settle for these messages names, when there was one
 * @param head the message to work — present exactly when a claim was granted, so this and not an empty
 *             list is what says whether the wait found anything
 * @param tail the rest of the batch, in producer order after `head`
 * @param leaseExpiresAt when the whole claim lapses unless renewed
 * @param backlogDepth how many more were queued for this key behind the batch
 */
final case class DequeueResponse(
  receipt: Option[ClaimRef],
  head: Option[Delivery],
  tail: Chunk[Delivery],
  leaseExpiresAt: Option[Instant],
  backlogDepth: Int,
)


/**
 * The outcome of a settle.
 *
 * @param applied whether it landed, or the claim had already been revoked
 */
final case class SettleResponse(applied: Applied)

/**
 * The outcome of a heartbeat.
 *
 * @param stale the receipts the caller no longer holds; it must stop working those
 * @param renewedUntil the new deadline for everything else, on the store's clock
 */
final case class HeartbeatResponse(stale: Chunk[ClaimRef], renewedUntil: Instant)
