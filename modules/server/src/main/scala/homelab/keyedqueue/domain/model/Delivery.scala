package homelab.keyedqueue.domain.model


import homelab.keyedqueue.domain.types.ClaimRef

import java.time.Instant


/**
 * A message handed to a consumer, with everything it needs to keep or settle it.
 *
 * @param receipt the opaque handle to give back to settle or renew
 * @param message the message
 * @param attempt how many times this message has been delivered; 1 on the first
 * @param leaseExpiresAt when the claim lapses unless renewed, on the store's clock
 * @param backlogDepth how many messages were still queued for this key when this one was claimed; non-zero
 *                     means something newer exists, which is what a latest-wins consumer acts on
 */
final case class Delivery(
  receipt: ClaimRef,
  message: Message,
  attempt: Int,
  leaseExpiresAt: Instant,
  backlogDepth: Int,
)
