package homelab.keyedqueue.domain.request.v1

/**
 * A caller's unchecked ask to release a lock.
 *
 * @param receipt the opaque handle from an acquire, as it arrived
 */
final case class ReleaseRequest(receipt: String)
