package homelab.keyedqueue.domain.model.lock


import homelab.keyedqueue.domain.types.LockName
import zio.Duration


/**
 * A caller's demand for a lock: which lock, how long to hold it, and how long to wait for it.
 *
 * The trusted counterpart of `AcquireRequest`, and bounded by construction — obtainable only from the
 * parse, so the store cannot be handed an unnamed lock or an hour-long wait.
 *
 * @param name the lock to take
 * @param ttl how long the hold survives without a refresh
 * @param patience how long to wait for a holder to release, at most what the service allows
 */
final case class Acquisition(name: LockName, ttl: Duration, patience: Duration)
