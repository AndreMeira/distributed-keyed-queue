package homelab.keyedqueue.domain.request.v1

import zio.Duration

/**
 * A caller's unchecked ask to take a lock: which lock, how long to hold it, how long to wait for it.
 *
 * What it asks for is a preference, not a promise. Parsing refuses an unnamed lock, a non-positive hold,
 * or no patience, and clamps the patience to what the service allows.
 *
 * @param name the lock to take, as it arrived
 * @param ttl how long to hold it without a refresh
 * @param maxWait how long to wait for a holder to release
 */
final case class AcquireRequest(name: String, ttl: Duration, maxWait: Duration)
