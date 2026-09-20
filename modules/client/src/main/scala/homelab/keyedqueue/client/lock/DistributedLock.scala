package homelab.keyedqueue.client.lock


import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.lock.managed.ManagedDistributedLock
import homelab.keyedqueue.client.lock.model.Hold
import zio.*


/**
 * A lock as a scope: take it, run something under it, give it back.
 *
 * The managed form of [[LockClient]]. It keeps the lease alive for as long as the caller's effect runs and
 * releases on every exit, so the receipt and the fence stay out of the caller's way. A caller that stamps
 * its writes with the fence wants [[LockClient]] instead, which hands the [[Hold]] over.
 */
trait DistributedLock:

  /**
   * Run `effect` holding the named lock, waiting up to `maxWait` for it.
   *
   * The lease is pushed forward while `effect` runs, and the lock is released when it ends — including
   * when it fails or is interrupted. A hold lost mid-run leaves `effect` running: this promises
   * mutual exclusion between callers that both take the lock, and the service's fence is what a caller
   * enforces downstream when it needs more than that.
   *
   * @param name the lock to take
   * @param ttl how long each lease runs, which also sets how often it is pushed forward
   * @param maxWait how long to wait for a holder to release
   * @param effect what to run while holding it
   * @tparam R what `effect` needs
   * @tparam E what `effect` aborts with
   * @tparam A what `effect` answers
   * @return what `effect` answered, or `None` when the wait elapsed with the lock still held; aborts with
   *         `effect`'s own error, or with a [[ServiceError]] when the service could not be reached
   */
  def acquire[R, E, A](
    name: String,
    ttl: Duration,
    maxWait: Duration,
  )(
    effect: ZIO[R, E, A]
  ): ZIO[R, ServiceError | E, Option[A]]

  /**
   * Run `effect` holding the named lock, only if it is free and unqueued right now.
   *
   * The non-waiting form, with the same lease handling as [[acquire]].
   *
   * @param name the lock to take
   * @param ttl how long each lease runs
   * @param effect what to run while holding it
   * @tparam R what `effect` needs
   * @tparam E what `effect` aborts with
   * @tparam A what `effect` answers
   * @return what `effect` answered, or `None` when somebody holds the lock or is queued for it; aborts
   *         with `effect`'s own error, or with a [[ServiceError]] when the service could not be reached
   */
  def tryAcquire[R, E, A](
    name: String,
    ttl: Duration,
  )(
    effect: ZIO[R, E, A]
  ): ZIO[R, ServiceError | E, Option[A]]


object DistributedLock:

  /**
   * The managed form over a client.
   *
   * @param client what it takes the lock with
   * @return the lock
   */
  def apply(client: LockClient): DistributedLock = ManagedDistributedLock(client)
