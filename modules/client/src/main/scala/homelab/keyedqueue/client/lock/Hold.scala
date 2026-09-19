package homelab.keyedqueue.client.lock

import java.time.Instant

/**
 * A lock this caller holds.
 *
 * @param receipt what releasing and refreshing name
 * @param fence the token to stamp downstream writes with
 * @param leaseExpiresAt when the hold lapses without a refresh, on the service's clock
 */
final case class Hold(receipt: Receipt, fence: Fence, leaseExpiresAt: Instant)
