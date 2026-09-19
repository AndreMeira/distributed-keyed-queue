package homelab.keyedqueue.domain.response.lock


import homelab.keyedqueue.domain.model.lock.Claim
import homelab.keyedqueue.domain.types.Token
import zio.Duration

import java.time.Instant


/**
 * The outcome of an acquire: the lock granted, or nothing. Unavailable is an ordinary result of a lock held
 * past the wait, not an error.
 *
 * An enum rather than a record of options, for the reason `DequeueResponse` is: the fields are not
 * independently optional — either the lock was granted, with a claim and a lease, or it was not.
 */
enum AcquireResponse:

  /** The wait elapsed with the lock still held. */
  case Unavailable

  /**
   * The lock was granted.
   *
   * @param receipt the handle to release or refresh with
   * @param token the fence this hold was granted under, to stamp downstream writes with
   * @param leaseUntil when the hold lapses unless refreshed
   * @param leaseTtl how long the lease runs, which is at most the hold the request asked for
   */
  case Granted(receipt: Claim.Ref, token: Token, leaseUntil: Instant, leaseTtl: Duration)
