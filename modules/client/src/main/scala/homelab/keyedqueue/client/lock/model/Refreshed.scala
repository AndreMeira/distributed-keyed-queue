package homelab.keyedqueue.client.lock.model


import zio.Duration

import java.time.Instant


/**
 * What pushing a lease forward answered.
 */
enum Refreshed:

  /**
   * The lease now runs to a later deadline.
   *
   * @param leaseExpiresAt the new deadline, on the service's clock
   * @param leaseTtl how long the new lease runs, which is at most what was asked for
   */
  case Renewed(leaseExpiresAt: Instant, leaseTtl: Duration)

  /** The hold is gone: this caller must stop acting as the holder. */
  case Lost
