package homelab.keyedqueue.client.lock.model

/**
 * The fencing token a grant runs under: a number that only ever rises.
 *
 * A holder stamps its writes to the protected resource with this, and the resource refuses a write
 * carrying an older one. That is what keeps a long hold safe for a holder that paused and lost the lock
 * without noticing.
 */
type Fence = Fence.Type


object Fence:
  opaque type Type <: Long = Long

  /**
   * A token as the service issued it.
   *
   * @param value the counter value
   * @return the fence
   */
  def apply(value: Long): Type = value
