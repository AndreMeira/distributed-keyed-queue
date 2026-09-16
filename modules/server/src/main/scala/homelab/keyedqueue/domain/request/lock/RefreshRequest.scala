package homelab.keyedqueue.domain.request.lock

import zio.Duration

/**
 * A caller's unchecked ask to extend a held lock's lease.
 *
 * @param receipt the opaque handle from an acquire, as it arrived
 * @param ttl how much longer to grant
 */
final case class RefreshRequest(receipt: String, ttl: Duration)
