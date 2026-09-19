package homelab.keyedqueue.client.lock


import homelab.keyedqueue.client.LockError
import zio.*


/**
 * The managed lock over a client.
 *
 * Both verbs are the same three steps — take it, run under it, give it back — differing only in which
 * call takes it, so the holding is written once.
 *
 * @param client what it takes the lock with
 */
final private[client] class ManagedLock(client: LockClient) extends DistributedLock:

  /**
   * Wait for the lock, then run `effect` under it.
   *
   * The scope opened here is what the renewals and the release are registered on, so both end when
   * `effect` does, however it ends.
   *
   * @param name the lock to take
   * @param ttl how long each lease runs
   * @param maxWait how long to wait for a holder to release
   * @param effect what to run while holding it
   * @tparam R what `effect` needs
   * @tparam E what `effect` aborts with
   * @tparam A what `effect` answers
   * @return what `effect` answered, or `None` when the wait elapsed; aborts with `effect`'s own error, or
   *         with a [[LockError]] when the service could not be reached
   */
  override def acquire[R, E, A](
    name: String,
    ttl: Duration,
    maxWait: Duration,
  )(
    effect: ZIO[R, E, A]
  ): ZIO[R, LockError | E, Option[A]] = ZIO.scoped:
    client.acquire(name, ttl, maxWait).flatMap {
      case Acquired.Unavailable   => ZIO.none
      case Acquired.Granted(hold) => holding(hold, ttl) *> effect.map(Some(_))
    }

  /**
   * Take the lock if it is free now, then run `effect` under it.
   *
   * The same scope and the same lease handling as [[acquire]]; only the call that takes the lock differs.
   *
   * @param name the lock to take
   * @param ttl how long each lease runs
   * @param effect what to run while holding it
   * @tparam R what `effect` needs
   * @tparam E what `effect` aborts with
   * @tparam A what `effect` answers
   * @return what `effect` answered, or `None` when the lock was not free; aborts with `effect`'s own
   *         error, or with a [[LockError]] when the service could not be reached
   */
  override def tryAcquire[R, E, A](
    name: String,
    ttl: Duration,
  )(
    effect: ZIO[R, E, A]
  ): ZIO[R, LockError | E, Option[A]] = ZIO.scoped:
    client.tryAcquire(name, ttl).flatMap {
      case Acquired.Unavailable   => ZIO.none
      case Acquired.Granted(hold) => holding(hold, ttl) *> effect.map(Some(_))
    }

  /**
   * Keep the hold alive for the rest of the scope, and give the lock back when the scope ends.
   *
   * Both are scope finalizers, and they run last-registered-first: the renewals stop, then the lock goes
   * back, so a caller's final refresh never races its own release. Registering the release on the scope
   * rather than on this effect is what carries it past a failure or an interruption inside the caller's
   * own work.
   *
   * @param hold the grant to keep alive
   * @param ttl how long each lease runs
   * @return noop once the renewals are running and the release is registered
   */
  private def holding(hold: Hold, ttl: Duration): ZIO[Scope, Nothing, Unit] =
    ZIO.addFinalizer(client.release(hold.receipt).ignore)
      *> renewing(hold, ttl).forkScoped.unit

  /**
   * Push the lease forward for as long as the service keeps granting it.
   *
   * Each renewal is asked for half a lease before the current one lapses, which leaves a full half-lease
   * of room for a slow call. The loop ends when the hold is lost or a call fails, and the lease itself is
   * what bounds a hold whose renewals stopped.
   *
   * @param hold the grant to keep alive
   * @param ttl how long each lease runs
   * @return noop when the renewals end
   */
  private def renewing(hold: Hold, ttl: Duration): UIO[Unit] =
    val interval = ttl.dividedBy(2)
    client
      .refresh(hold.receipt, ttl)
      .delay(interval)
      .repeat(Schedule.spaced(interval) && Schedule.recurWhile(stillHeld))
      .ignore

  /**
   * Whether a renewal means the hold is still this caller's.
   *
   * @param refreshed what the service answered
   * @return whether to keep renewing
   */
  private def stillHeld(refreshed: Refreshed): Boolean =
    refreshed match
      case Refreshed.Renewed(_) => true
      case Refreshed.Lost       => false
