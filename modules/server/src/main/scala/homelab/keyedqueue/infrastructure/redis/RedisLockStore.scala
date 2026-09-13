package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.model.{ Acquisition, LockClaim }
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.lock.LockStore.Hold
import homelab.keyedqueue.domain.types.{ LockName, QueueName }
import homelab.keyedqueue.infrastructure.redis.script.{ LockAcquireScript, LockGrantScript, LockTryScript }
import zio.*

import java.time.Instant


/**
 * The lock over Redis: fair by ticket, no sweep.
 *
 * '''Order lives in the store.''' A blocking acquire that cannot be granted at once takes a tail ticket in
 * the lock's waiters list, and grants follow ticket order among tickets still within their patience — so
 * the instances keep no waiter state, and a newcomer cannot barge past the queue ([[tryAcquire]] refuses
 * when live tickets exist). A dead holder is reclaimed inline by the next grant, a dead waiter is pruned at
 * its own deadline; neither needs a background pass.
 *
 * '''Waiters park until a known event, not on a poll.''' Every refusal names the delay after which the
 * answer can change — the lease's end when the lock is held, the head ticket's deadline when queued behind
 * it — and the waiter parks on its [[Broadcast]] mailbox for at most that long. The wake is
 * cross-instance: release and trim append to a wake stream in the same script that frees the lock, and the
 * shared [[WakeListener]] delivers it to every instance's broadcast; every local waiter wakes and asks,
 * only the head ticket can win, so the woken crowd is a check, not a race. The mailbox is subscribed
 * before the enter, so no release can slip into the gap between asking and parking.
 *
 * @param monitor what each call on the substrate is traced against
 * @param connection where its connection comes from
 * @param scripts the loaded lock scripts
 * @param broadcast where a waiter's wakes land
 */
final class RedisLockStore(
  monitor: Monitor,
  connection: Connection,
  scripts: LockScripts,
  broadcast: Broadcast,
) extends LockStore:

  /** The least a waiter parks between grant attempts, so clock-boundary refusals cannot spin. */
  private val floor: Duration = 10.millis

  /**
   * One `try` call, which answers at once.
   *
   * Refuses a lock that is free but queued for: a live ticket means someone asked first, and granting past
   * them is the barging the tickets exist to end. The script names the token and the deadline; naming the
   * lock is this adapter's job, since only the caller knows which it asked for.
   *
   * @param acquisition the lock to take and how long to hold it; its patience is ignored here
   * @return the hold, or `None` when the lock is held or queued for
   */
  override def tryAcquire(acquisition: Acquisition): IO[RedisFailure, Option[Hold]] =
    monitor.trace("RedisLockStore.tryAcquire"):
      connection.provide:
        scripts.tryAcquire
          .execute(acquisition.name, acquisition.ttl)
          .map(_.map(hold(acquisition.name)))

  /**
   * Enter for the lock, and wait out the turns until it is this caller's or the patience is spent.
   *
   * The mailbox is subscribed '''before''' entering, so a release cannot fall into the gap between asking
   * and parking. What comes back is either the lock or a ticket; the ticket's wait is [[queued]].
   *
   * @param acquisition the lock to take, how long to hold it, and how long to wait
   * @return the hold, or `None` when the patience elapsed first
   */
  override def acquire(acquisition: Acquisition): IO[RedisFailure, Option[Hold]] = ZIO.scoped:
    // The span covers the whole wait, so its duration is the caller's wait time — read it the way the
    // observability doc reads dequeue's, not as a processing latency.
    monitor.trace("RedisLockStore.acquire"):
      Clock.instant.flatMap: asked =>
        broadcast.subscribe(QueueName(acquisition.name)).flatMap { mailbox =>
          connection
            .provide:
              scripts.acquire.execute(acquisition.name, acquisition.ttl, acquisition.patience)
            .flatMap:
              case LockAcquireScript.Entered.Granted(token, until)   => ZIO.some(hold(acquisition.name)((token, until)))
              case LockAcquireScript.Entered.Queued(ticket, recheck) => queued(acquisition, asked, ticket, recheck, mailbox)
        }

  /**
   * One `release` call, which frees the lock and wakes whoever waits on it.
   *
   * @param claim the claim from the hold
   * @return true when released, false when the hold had already been revoked
   */
  override def release(claim: LockClaim): IO[RedisFailure, Boolean] =
    // The wake is the script's job: lock/release.lua appends to the wake stream on a real release, and the
    // shared listener delivers it to every instance's readiness — so a waiter on another instance wakes,
    // which an in-process call could never reach.
    monitor.trace("RedisLockStore.release"):
      connection.provide(scripts.release.execute(claim.name, claim.token))

  /**
   * One `refresh` call, which moves the lease if the caller still holds the lock.
   *
   * Token-only, deliberately: a holder whose lease lapsed but whom nobody displaced is late, not lost. The
   * deadline it answers with is meaningless when the hold is gone, which is why the pair says both.
   *
   * @param claim the claim from the hold
   * @param ttl how much longer to grant
   * @return the new deadline, and whether the hold survived to take it
   */
  override def refresh(claim: LockClaim, ttl: Duration): IO[RedisFailure, (Instant, Boolean)] =
    monitor.trace("RedisLockStore.refresh"):
      connection.provide:
        scripts.refresh.execute(claim.name, claim.token, ttl).map(_.toTuple)

  /**
   * One `trim` call, which removes holds abandoned past the grace and the waiter lists left with them.
   *
   * Hygiene, not liveness: a lock somebody wants is reclaimed inline by the next grant, so what this frees
   * is what no request would ever reach.
   *
   * @param grace how long past lease expiry a hold survives before it may be removed
   * @param limit the most holds one pass removes
   * @return the names freed, oldest lease first
   */
  override def trim(grace: Duration, limit: Int): IO[RedisFailure, Chunk[LockName]] =
    monitor.trace("RedisLockStore.trim"):
      connection.provide:
        scripts.trim.execute(grace, limit)

  /**
   * The hold a granted reply amounts to.
   *
   * The reply carries the token and the deadline; only this caller knows which lock it asked for, which is
   * why the script answers with less than a `Hold`.
   *
   * @param name the lock that was asked for
   * @param granted the token the grant runs under, and when its lease lapses
   * @return the hold, as the port promises it
   */
  private def hold(name: LockName)(granted: LockTryScript.Granted): Hold =
    Hold(LockClaim(name, granted.token), granted.leaseUntil)

  /**
   * Hold a ticket to the end: wait out the turns, and withdraw it on any exit that is not a grant.
   *
   * The withdrawal is a finaliser, so a caller that is interrupted mid-wait leaves the queue rather than
   * a ticket that delays the tail until its deadline.
   *
   * @param acquisition the lock being entered for
   * @param asked when the call arrived, which the patience is measured from
   * @param ticket the ticket the enter minted
   * @param recheck the first delay the enter named
   * @return the hold, or `None` when the patience elapsed; aborts with `RedisFailure` when the store fails
   */
  private def queued(
    acquisition: Acquisition,
    asked: Instant,
    ticket: Long,
    recheck: Duration,
    mailbox: Queue[Unit],
  ): IO[RedisFailure, Option[Hold]] =
    for
      now       <- Clock.instant
      ticketRef <- Ref.make(ticket)
      recheckAt <- Ref.make(now.plus(atLeastFloor(recheck)))
      result    <- awaitTurn(acquisition, asked, ticketRef, recheckAt, mailbox).onExit {
                     case Exit.Success(Some(_)) => ZIO.unit
                     case _                     => withdraw(acquisition.name, ticketRef)
                   }
    yield result

  /**
   * Wait for this ticket's turn, until granted or the patience is spent — the ticketed twin of
   * `RedisQueueStore.claimWithin`.
   *
   * Parks until the earlier of the next known event and the remaining patience — a wake in the mailbox
   * cuts the park short — then asks. Every ask is deliberate: it follows a wake, the arrival of the next
   * known event, or the patience running out.
   *
   * @param acquisition the lock being entered for
   * @param asked when the call arrived, which the patience is measured from
   * @param ticket the ticket to ask with; re-minted when the queue no longer knows it
   * @param recheckAt when the answer can next change, kept by the grant attempts
   * @param mailbox where this waiter's wakes land
   * @return the hold, or `None` when the patience elapsed; aborts with `RedisFailure` when the store fails
   */
  private def awaitTurn(
    acquisition: Acquisition,
    asked: Instant,
    ticket: Ref[Long],
    recheckAt: Ref[Instant],
    mailbox: Queue[Unit],
  ): IO[RedisFailure, Option[Hold]] =
    remainingTime(acquisition.patience, asked).flatMap:
      case None       => ZIO.none
      case Some(left) =>
        untilRecheck(recheckAt, left).flatMap: window =>
          val park = mailbox.take.timeout(window).unless(window.isZero)
          (park *> turn(acquisition, asked, ticket, recheckAt)).flatMap:
            case taken @ Some(_) => ZIO.succeed(taken)
            case None            => awaitTurn(acquisition, asked, ticket, recheckAt, mailbox)

  /**
   * One grant attempt, keeping the ticket and the next event time current.
   *
   * A queue that no longer knows the ticket is re-entered: the caller keeps what is left of its patience
   * but loses its place, which is the honest reading of a pruned ticket.
   *
   * @param acquisition the lock being entered for
   * @param asked when the call arrived, which the patience is measured from
   * @param ticket the ticket to ask with
   * @param recheckAt where the next event time is kept
   * @return the hold, or `None` to keep waiting; aborts with `RedisFailure` when the store fails
   */
  private def turn(
    acquisition: Acquisition,
    asked: Instant,
    ticket: Ref[Long],
    recheckAt: Ref[Instant],
  ): IO[RedisFailure, Option[Hold]] =
    // One span per deliberate ask: their count per acquire is the wake-efficiency signal — an event or two
    // each, never a poll's worth.
    monitor.trace("RedisLockStore.grant"):
      ticket.get.flatMap: id =>
        connection
          .provide(scripts.grant.execute(acquisition.name, id, acquisition.ttl))
          .flatMap:
            case LockGrantScript.Asked.Granted(token, until) =>
              ZIO.some(hold(acquisition.name)((token = token, leaseUntil = until)))
            case LockGrantScript.Asked.Wait(delay)           => nextEventIn(delay, recheckAt).as(None)
            case LockGrantScript.Asked.Gone                  => reenter(acquisition, asked, ticket, recheckAt)

  /**
   * Enter again after the queue lost the ticket — with what is '''left''' of the patience, not all of it.
   *
   * The fresh ticket's deadline must state the caller's real bound: a full-patience ticket would hold a
   * place, and delay everyone reading the head's deadline, long after this caller has given up.
   *
   * @param acquisition the lock being entered for
   * @param asked when the call arrived, which the patience is measured from
   * @param ticket where the fresh ticket replaces the lost one
   * @param recheckAt where the next event time is kept
   * @return the hold when the re-enter granted at once, `None` to keep waiting — or at once, when the
   *         patience is already spent; aborts with `RedisFailure` when the store fails
   */
  private def reenter(
    acquisition: Acquisition,
    asked: Instant,
    ticket: Ref[Long],
    recheckAt: Ref[Instant],
  ): IO[RedisFailure, Option[Hold]] =
    remainingTime(acquisition.patience, asked).flatMap:
      case None       => ZIO.none
      case Some(left) =>
        connection
          .provide(scripts.acquire.execute(acquisition.name, acquisition.ttl, left))
          .flatMap:
            case LockAcquireScript.Entered.Granted(token, until)  =>
              ZIO.some(hold(acquisition.name)((token = token, leaseUntil = until)))
            case LockAcquireScript.Entered.Queued(fresh, recheck) =>
              ticket.set(fresh) *> nextEventIn(recheck, recheckAt).as(None)

  /**
   * Note when the answer can next change.
   *
   * @param delay what the script named
   * @param recheckAt where it is kept
   * @return noop
   */
  private def nextEventIn(delay: Duration, recheckAt: Ref[Instant]): UIO[Unit] =
    Clock.instant.flatMap(now => recheckAt.set(now.plus(atLeastFloor(delay))))

  /**
   * How long to park before the next known event, bounded by the patience left.
   *
   * @param recheckAt when the answer can next change
   * @param left the patience remaining
   * @return the park; zero when the event time has already passed
   */
  private def untilRecheck(recheckAt: Ref[Instant], left: Duration): UIO[Duration] =
    recheckAt.get.zipWith(Clock.instant): (at, now) =>
      val until = Duration.fromInterval(now, at)
      if until.toMillis <= 0 then Duration.Zero
      else if until.toMillis < left.toMillis then until
      else left

  /**
   * A delay no shorter than the spin floor.
   *
   * @param delay what the script named
   * @return that, or the floor, whichever is longer
   */
  private def atLeastFloor(delay: Duration): Duration =
    if delay.toMillis < floor.toMillis then floor else delay

  /**
   * Withdraw the ticket, best effort.
   *
   * @param name the lock queued for
   * @param ticket the ticket to withdraw
   * @return noop; a withdrawal that fails is left to the deadline prune
   */
  private def withdraw(name: LockName, ticket: Ref[Long]): UIO[Unit] =
    ticket.get.flatMap(id => connection.provide(scripts.abandon.execute(name, id)).ignore)

  /**
   * What is left of the caller's patience.
   *
   * @param patience what it was granted
   * @param asked when its call arrived
   * @return the time still to wait, or `None` when it is spent
   */
  private def remainingTime(patience: Duration, asked: Instant): UIO[Option[Duration]] =
    Clock.instant.map: now =>
      val left = patience.minus(Duration.fromInterval(asked, now))
      Option.when(left.toMillis > 0)(left)


object RedisLockStore:

  /**
   * Load the lock scripts and hand back the store.
   *
   * @param monitor what each call on the substrate is traced against
   * @param connection where its connection comes from
   * @param broadcast where waiters' wakes land
   * @return the store; aborts with `RedisFailure` when a script is missing or rejected
   */
  def make(
    monitor: Monitor,
    connection: Connection,
    broadcast: Broadcast,
  ): ZIO[Connection.Commands, RedisFailure, RedisLockStore] =
    LockScripts.make.map(RedisLockStore(monitor, connection, _, broadcast))
