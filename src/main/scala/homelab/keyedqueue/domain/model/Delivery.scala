package homelab.keyedqueue.domain.model


import homelab.keyedqueue.domain.types.Receipt

import java.time.Instant


/**
 * A message handed to a consumer, with everything it needs to keep or settle it.
 *
 * @param receipt the opaque handle to give back to settle or renew
 * @param envelope the message
 * @param attempt how many times this message has been delivered; 1 on the first
 * @param leaseExpiresAt when the claim lapses unless renewed, on the store's clock
 */
final case class Delivery(receipt: Receipt, envelope: Envelope, attempt: Int, leaseExpiresAt: Instant)
