package homelab.keyedqueue.domain.service.lock


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.*
import homelab.keyedqueue.domain.model.Message.Encoding
import homelab.keyedqueue.domain.model.Settlement.Verdict
import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import zio.*

import java.time.Instant


/**
 * A distributed lock built on the queue, with nothing new in the store.
 *
 * '''DKQ as a client of itself.''' A lock is a queue holding one immortal '''baton''' message; per-key
 * exclusivity is the mutual exclusion, `claim` is the acquire, and a nack with no backoff is the release.
 * Acquire, release and refresh are compositions of operations the store already has — which is why this
 * carries no scripts, no structures and no wake flow of its own. See
 * `docs/research/distributed-lock.md`.
 *
 * Written against [[QueueStore]] so it can be exercised against the same substrate the queue uses; the
 * production form could equally be a pure gRPC client, leaving the server untouched.
 *
 * '''One lock, one queue.''' The store's `claim` takes the oldest claimable key, not a named one, so the
 * only way "acquire lock L" is unambiguous is for L to be its own queue with a single key. The lock's name
 * is the queue name.
 *
 * '''The baton is never acked.''' Release is always a nack with no backoff, so the message stays in place
 * and its fixed id makes every bootstrap [[acquire]] a dedup no-op — which is what guarantees exactly one
 * baton, and so at most one holder, even when two callers race to create the lock.
 *
 * '''Crash-recovery is the watchdog's, inherited.''' A held lock whose holder dies is returned by the same
 * sweep that reclaims a dead consumer's claim — so [[acquire]] registers the lock's queue with the
 * watchdog exactly as `DequeueUseCase` does, and nothing lock-specific is needed. In the pure-client form
 * this is automatic: the server's dequeue handler watches the queue, so an external lock client gets
 * recovery for free; the dependency appears here only because this sketch drives the store directly.
 *
 * @param store the queue this lock is a client of
 * @param watchdog told which lock queues exist, so an abandoned hold is swept back to free
 * @param leaseTtl how long a held lock survives without a [[refresh]] — the store's own lease
 */
final class DistributedLock(store: QueueStore, watchdog: Watchdog, leaseTtl: Duration):
  import DistributedLock.*

  /**
   * Take the lock, waiting up to `patience` for a holder to release it.
   *
   * Bootstraps the baton (idempotent: a second caller's enqueue dedups to a no-op while the lock is held),
   * then claims the lock's single key — blocking on the store's own readiness path until it is free, or
   * the patience elapses.
   *
   * @param name the lock to take
   * @param patience the longest to wait for it
   * @return the hold, carrying the fence token to release or refresh with; `None` when the wait elapsed
   *         first; aborts with an `AdapterError` when the store fails
   */
  def acquire(name: LockName, patience: Duration): IO[ApplicationError.AdapterError, Option[Held]] =
    ensureBaton(name) *> watchdog.watch(queue(name)) *> store
      .claim(Demand(queue(name), patience, batch = 1))
      .map(_.map(grant => Held(grant.claim)))

  /**
   * Take the lock only if it is free now.
   *
   * '''"Now" is a brief look, not zero.''' The store's `claim` requires positive patience — a dequeue that
   * will not wait is refused at the API boundary as invalid, so a client of the store cannot ask for none.
   * `tryAcquire` uses the smallest patience that still looks, which is a real (tiny) wait, not a
   * `CAS`-style instant test. A substrate-native lock would express a true zero-wait try; the self-client
   * form inherits the queue's "you must be willing to wait" contract.
   *
   * @param name the lock to take
   * @return the hold, or `None` when it is held; aborts with an `AdapterError` when the store fails
   */
  def tryAcquire(name: LockName): IO[ApplicationError.AdapterError, Option[Held]] =
    acquire(name, DistributedLock.briefLook)

  /**
   * Release a lock this caller holds, so the next waiter may take it.
   *
   * A nack with no backoff: the baton returns to the claimable set at once and a waiter is woken. Fenced —
   * a hold whose lease already lapsed and was reclaimed applies nothing.
   *
   * @param held the hold returned by [[acquire]]
   * @return true when released, false when the hold had already been revoked; aborts with an `AdapterError`
   *         when the store fails
   */
  def release(held: Held): IO[ApplicationError.AdapterError, Boolean] =
    store.settle(Settlement(held.claim, NonEmptyChunk(Settlement.Outcome(batonId, Verdict.Failed)), retryAfter = None))

  /**
   * Push the lease forward on a lock this caller holds.
   *
   * @param held the hold to keep alive
   * @return the new deadline, and whether the hold is still valid (false when it has been revoked); aborts
   *         with an `AdapterError` when the store fails
   */
  def refresh(held: Held): IO[ApplicationError.AdapterError, (Instant, Boolean)] =
    store.renew(Chunk(held.claim)).map((until, lost) => (until, lost.isEmpty))

  /**
   * Take the lock, run `use`, and release it whatever happens — the bracketed form callers usually want.
   *
   * @param name the lock to take
   * @param patience the longest to wait for it
   * @param use what to do while holding it
   * @tparam R what `use` needs
   * @tparam A what `use` produces
   * @return `use`'s result wrapped in `Some`, or `None` when the lock could not be taken in time; aborts
   *         with an `AdapterError` when the store fails, or with `use`'s own error
   */
  def withLock[R, A](
    name: LockName,
    patience: Duration,
  )(
    use: IO[ApplicationError.AdapterError, A]
  ): IO[ApplicationError.AdapterError, Option[A]] =
    ZIO.acquireReleaseWith(acquire(name, patience))(held => ZIO.foreachDiscard(held)(release(_).ignore)) {
      case Some(_) => use.asSome
      case None    => ZIO.none
    }

  /**
   * Make sure the lock's baton exists, without disturbing a held lock.
   *
   * @param name the lock
   * @return noop; aborts with an `AdapterError` when the store fails
   */
  private def ensureBaton(name: LockName): IO[ApplicationError.AdapterError, Unit] =
    store.enqueue(Submission(queue(name), baton)).unit


object DistributedLock:

  /** A lock's name — the queue that stands for it. */
  type LockName = LockName.Type
  object LockName:
    opaque type Type <: String = String
    def apply(value: String): Type = value

  /** The single key every lock queue holds; the queue has one, so its identity does not matter. */
  private val batonKey: MessageKey = MessageKey("lock")

  /** The baton's fixed id: fixed so a re-enqueue dedups, which is what keeps the baton unique. */
  private val batonId: MessageId = MessageId("baton")

  /** The immortal message a lock circulates. Empty cargo — a lock carries no payload, only the right to hold. */
  private val baton: Message = Message(batonKey, batonId, "dkq.lock/baton", Encoding.Json, None, Chunk.empty)

  /** The shortest patience that still looks once — the store refuses a claim that will not wait at all. */
  private val briefLook: Duration = 50.millis

  private def queue(name: LockName): QueueName = QueueName(name)

  /**
   * A held lock: the claim that authorises releasing or refreshing it.
   *
   * @param claim the store claim behind the hold — the fence token a caller must present
   */
  final case class Held(claim: Claim)

  /**
   * A lock over `store`, swept by `watchdog`.
   *
   * @param store the queue to build on
   * @param watchdog the running sweep that reclaims abandoned holds
   * @param leaseTtl how long a hold survives without a refresh
   * @return the lock
   */
  def make(store: QueueStore, watchdog: Watchdog, leaseTtl: Duration): DistributedLock =
    DistributedLock(store, watchdog, leaseTtl)
