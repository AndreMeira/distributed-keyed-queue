package homelab.keyedqueue.domain.model.lock

import java.time.Instant

/**
 * A held lock: the claim that authorises releasing or refreshing it, and the current lease.
 *
 * @param claim which lock, under which fence generation — and the handle a caller carries
 * @param leaseUntil when the hold lapses unless refreshed, on the store's clock
 */
final case class Hold(claim: Claim, leaseUntil: Instant)
