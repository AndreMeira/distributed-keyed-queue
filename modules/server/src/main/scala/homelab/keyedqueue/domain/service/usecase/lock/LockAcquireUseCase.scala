package homelab.keyedqueue.domain.service.usecase.lock


import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.common.orFail
import homelab.keyedqueue.domain.request.lock.AcquireRequest
import homelab.keyedqueue.domain.response.lock.AcquireResponse
import homelab.keyedqueue.domain.model.Acquisition
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.lock.LockStore.{ Asked, Entered }
import homelab.keyedqueue.domain.service.readiness.LockReadiness
import homelab.keyedqueue.domain.types.{ LockName, Ticket }
import homelab.keyedqueue.domain.service.validation.LockInputValidation
import zio.*

import java.time.Instant


/**
 * Take a lock, blocking until it is free or the wait elapses.
 *
 * @param store where the lock lives
 * @param validation what turns a request into an acquisition this service will honour
 */
final class LockAcquireUseCase(store: LockStore, validation: LockInputValidation, readiness: LockReadiness):

  /**
   * Parse, then acquire.
   *
   * Nothing acquired in time is a *response*, not a failure: a caller that waited its patience for a held
   * lock has behaved exactly as asked.
   *
   * @param request the lock and what the caller is asking for, untrusted
   * @return the grant, or nothing when the wait elapsed; aborts with `ValidationError` when the request is
   *         malformed, or with `ApplicationError` when the store fails
   */
  def apply(request: AcquireRequest): IO[ApplicationError, AcquireResponse] =
    validation.parse(request).orFail.flatMap(acquire).map {
      case Some(hold) => AcquireResponse.Granted(hold.claim, hold.leaseUntil)
      case None       => AcquireResponse.Unavailable
    }

  /**
   * Take the lock, waiting out the turns until it is this caller's or the patience is spent.
   *
   * The mailbox is subscribed before entering, so a release cannot land in the gap between asking and
   * parking.
   *
   * @param acquisition the lock to take, how long to hold it, and how long to wait
   * @return the hold, or `None` when the patience elapsed first; aborts with an `AdapterError` when the
   *         store fails
   */
  private def acquire(acquisition: Acquisition): IO[ApplicationError.AdapterError, Option[LockStore.Hold]] =
    ZIO.scoped:
      for
        asked   <- Clock.instant
        mailbox <- readiness.subscribe(acquisition.name)
        entered <- store.enter(acquisition, acquisition.patience)
        held    <- entered match
                     case Entered.Granted(hold)           => ZIO.some(hold)
                     case Entered.Queued(ticket, recheck) => queued(acquisition, asked, ticket, recheck, mailbox)
      yield held

  /**
   * Hold a ticket to the end: wait out the turns, and withdraw it on any exit that is not a grant.
   *
   * The withdrawal is a finaliser, so a caller interrupted mid-wait leaves the queue rather than a ticket
   * that delays the tail until its deadline.
   *
   * @param acquisition the lock being entered for
   * @param asked when the call arrived, which the patience is measured from
   * @param ticket the ticket the enter minted
   * @param recheck the first delay the enter named
   * @param mailbox where this waiter's wakes land
   * @return the hold, or `None` when the patience elapsed; aborts with an `AdapterError` when the store fails
   */
  private def queued(
    acquisition: Acquisition,
    asked: Instant,
    ticket: Ticket,
    recheck: Duration,
    mailbox: Queue[Unit],
  ): IO[ApplicationError.AdapterError, Option[LockStore.Hold]] =
    for
      now       <- Clock.instant
      ticketRef <- Ref.make(ticket)
      recheckAt <- Ref.make(now.plus(atLeastFloor(recheck)))
      result    <- awaitTurn(acquisition, asked, ticketRef, recheckAt, mailbox).onExit:
                     case Exit.Success(Some(_)) => ZIO.unit
                     case _                     => withdraw(acquisition.name, ticketRef)
    yield result

  /**
   * Wait for this ticket's turn, until granted or the patience is spent.
   *
   * Parks until the earlier of the next known event and the remaining patience — a wake in the mailbox cuts
   * the park short — then asks. Every ask is deliberate: it follows a wake, the arrival of the next known
   * event, or the patience running out.
   *
   * @param acquisition the lock being entered for
   * @param asked when the call arrived, which the patience is measured from
   * @param ticket the ticket to ask with; re-minted when the queue no longer knows it
   * @param recheckAt when the answer can next change
   * @param mailbox where this waiter's wakes land
   * @return the hold, or `None` when the patience elapsed; aborts with an `AdapterError` when the store fails
   */
  private def awaitTurn(
    acquisition: Acquisition,
    asked: Instant,
    ticket: Ref[Ticket],
    recheckAt: Ref[Instant],
    mailbox: Queue[Unit],
  ): IO[ApplicationError.AdapterError, Option[LockStore.Hold]] =
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
   * A queue that no longer knows the ticket is entered again: the caller keeps what is left of its patience
   * but loses its place, which is the honest reading of a pruned ticket.
   *
   * @param acquisition the lock being entered for
   * @param asked when the call arrived, which the patience is measured from
   * @param ticket the ticket to ask with
   * @param recheckAt where the next event time is kept
   * @return the hold, or `None` to keep waiting; aborts with an `AdapterError` when the store fails
   */
  private def turn(
    acquisition: Acquisition,
    asked: Instant,
    ticket: Ref[Ticket],
    recheckAt: Ref[Instant],
  ): IO[ApplicationError.AdapterError, Option[LockStore.Hold]] =
    ticket.get
      .flatMap(store.grant(acquisition, _))
      .flatMap:
        case Asked.Granted(hold) => ZIO.some(hold)
        case Asked.Wait(delay)   => nextEventIn(delay, recheckAt).as(None)
        case Asked.Gone          => reenter(acquisition, asked, ticket, recheckAt)

  /**
   * Enter again after the queue lost the ticket, with what is left of the patience.
   *
   * The fresh ticket's deadline states the caller's real bound, so it stops delaying the tail once this
   * caller has given up.
   *
   * @param acquisition the lock being entered for
   * @param asked when the call arrived, which the patience is measured from
   * @param ticket where the fresh ticket replaces the lost one
   * @param recheckAt where the next event time is kept
   * @return the hold when the re-enter granted at once, `None` to keep waiting; aborts with an
   *         `AdapterError` when the store fails
   */
  private def reenter(
    acquisition: Acquisition,
    asked: Instant,
    ticket: Ref[Ticket],
    recheckAt: Ref[Instant],
  ): IO[ApplicationError.AdapterError, Option[LockStore.Hold]] =
    remainingTime(acquisition.patience, asked).flatMap:
      case None       => ZIO.none
      case Some(left) =>
        store.enter(acquisition, left).flatMap {
          case Entered.Granted(hold)          => ZIO.some(hold)
          case Entered.Queued(fresh, recheck) => ticket.set(fresh) *> nextEventIn(recheck, recheckAt).as(None)
        }

  /**
   * Withdraw the ticket, best effort.
   *
   * @param name the lock queued for
   * @param ticket the ticket to withdraw
   * @return noop; a withdrawal that fails is left to the deadline prune
   */
  private def withdraw(name: LockName, ticket: Ref[Ticket]): UIO[Unit] =
    ticket.get.flatMap(store.withdraw(name, _)).ignore

  /**
   * Note when the answer can next change.
   *
   * @param delay what the store named
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
   * @param delay what the store named
   * @return that, or the floor, whichever is longer
   */
  private def atLeastFloor(delay: Duration): Duration =
    if delay.toMillis < LockAcquireUseCase.floor.toMillis then LockAcquireUseCase.floor else delay

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


object LockAcquireUseCase:

  /** The least a waiter parks between grant attempts, so clock-boundary refusals cannot spin. */
  private val floor: Duration = 10.millis
