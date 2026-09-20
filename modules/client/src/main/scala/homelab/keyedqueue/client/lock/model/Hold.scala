package homelab.keyedqueue.client.lock.model


import zio.Duration

import java.time.Instant


/**
 * A lock this caller holds.
 *
 * @param receipt what releasing and refreshing name
 * @param fence the token to stamp downstream writes with
 * @param leaseExpiresAt when the hold lapses without a refresh, on the service's clock
 * @param leaseTtl how long the lease runs, which is at most what was asked for — the span a holder times
 *                 its refreshes by
 */
final case class Hold(receipt: Receipt, fence: Fence, leaseExpiresAt: Instant, leaseTtl: Duration)
