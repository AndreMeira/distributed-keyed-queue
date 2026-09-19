package homelab.keyedqueue.client.lock

import java.time.Instant


/**
 * What pushing a lease forward answered.
 */
enum Refreshed:

  /**
   * The lease now runs to a later deadline.
   *
   * @param leaseExpiresAt the new deadline, on the service's clock
   */
  case Renewed(leaseExpiresAt: Instant)

  /** The hold is gone: this caller must stop acting as the holder. */
  case Lost
