package homelab.keyedqueue.domain.request.lock

import zio.Duration

/**
 * A caller's unchecked ask to take a lock only if it is free now: which lock, and how long to hold it.
 *
 * There is no wait to state — the answer is about this instant. Parsing refuses an unnamed lock or a
 * non-positive hold, and clamps the hold to what the service allows.
 *
 * @param name the lock to take, as it arrived
 * @param ttl how long to hold it without a refresh
 */
final case class TryAcquireRequest(name: String, ttl: Duration)
