package homelab.keyedqueue.domain.response.lock

import java.time.Instant


/**
 * The outcome of a refresh: the lease extended, or the hold lost. Lost is an ordinary result — the lock was
 * reclaimed or handed on — not an error.
 */
enum RefreshResponse:

  /** The hold has been lost; the caller must stop treating the lock as held. */
  case Lost

  /**
   * The lease was extended.
   *
   * @param leaseUntil the new deadline
   */
  case Renewed(leaseUntil: Instant)
