package homelab.keyedqueue.client.lock


import homelab.keyedqueue.client.ServiceError
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
   * `effect` does, however it ends. Taking the lock and registering its release happen together, under a
   * mask, so a grant always reaches the finaliser that gives it back.
   *
   * @param name the lock to take
   * @param ttl how long to ask each lease to run, which the service may grant less of
   * @param maxWait how long to wait for a holder to release
   * @param effect what to run while holding it
   * @tparam R what `effect` needs
   * @tparam E what `effect` aborts with
   * @tparam A what `effect` answers
   * @return what `effect` answered, or `None` when the wait elapsed; aborts with `effect`'s own error, or
   *         with a [[ServiceError]] when the service could not be reached
   */
  override def acquire[R, E, A](
    name: String,
    ttl: Duration,
    maxWait: Duration,
  )(
    effect: ZIO[R, E, A]
  ): ZIO[R, ServiceError | E, Option[A]] = ZIO.scoped:
    ZIO.uninterruptibleMask: restore =>
      restore(client.acquire(name, ttl, maxWait)).flatMap {
        case Acquired.Unavailable   => ZIO.none
        case Acquired.Granted(hold) => holding(hold) *> restore(effect).map(Some(_))
      }

  /**
   * Take the lock if it is free now, then run `effect` under it.
   *
   * The same scope, mask and lease handling as [[acquire]]; only the call that takes the lock differs.
   *
   * @param name the lock to take
   * @param ttl how long to ask each lease to run, which the service may grant less of
   * @param effect what to run while holding it
   * @tparam R what `effect` needs
   * @tparam E what `effect` aborts with
   * @tparam A what `effect` answers
   * @return what `effect` answered, or `None` when the lock was not free; aborts with `effect`'s own
   *         error, or with a [[ServiceError]] when the service could not be reached
   */
  override def tryAcquire[R, E, A](
    name: String,
    ttl: Duration,
  )(
    effect: ZIO[R, E, A]
  ): ZIO[R, ServiceError | E, Option[A]] = ZIO.scoped:
    ZIO.uninterruptibleMask: restore =>
      restore(client.tryAcquire(name, ttl)).flatMap {
        case Acquired.Unavailable   => ZIO.none
        case Acquired.Granted(hold) => holding(hold) *> restore(effect).map(Some(_))
      }

  /**
   * Keep the hold alive for the rest of the scope, and give the lock back when the scope ends.
   *
   * Both are scope finalizers, and they run last-registered-first: the renewals stop, then the lock goes
   * back, so a caller's final refresh never races its own release. The release sits on the scope, so it
   * runs on whatever ends the caller's work — an answer, a failure, an interruption. The renewals are
   * forked interruptible so the finaliser that stops them can, since a fiber inherits the interruptibility
   * it was forked under and this runs inside a mask.
   *
   * @param hold the grant to keep alive
   * @return noop once the renewals are running and the release is registered
   */
  private def holding(hold: Hold): ZIO[Scope, Nothing, Unit] =
    ZIO.addFinalizer(client.release(hold.receipt).ignore)
      *> renewing(hold.receipt, hold.leaseTtl).ignore.forkScoped.interruptible.unit

  /**
   * Push the lease forward for as long as the service keeps granting it.
   *
   * Each renewal falls due half a lease before the current one lapses, leaving a full half-lease of room
   * for a slow call, and asks for the span it currently holds. Every answer carries the lease it granted,
   * and that is what times the one after it, so a ceiling that moves under a running hold is followed.
   *
   * @param receipt what the renewals name
   * @param lease how long the lease now runs, which is what the next renewal is timed by
   * @return noop when the hold is lost; aborts with a [[ServiceError]] when a call fails
   */
  private def renewing(receipt: Receipt, lease: Duration): IO[ServiceError, Unit] =
    client.refresh(receipt, lease).delay(lease.dividedBy(2)).flatMap {
      case Refreshed.Lost                => ZIO.unit
      case Refreshed.Renewed(_, granted) => renewing(receipt, granted)
    }
