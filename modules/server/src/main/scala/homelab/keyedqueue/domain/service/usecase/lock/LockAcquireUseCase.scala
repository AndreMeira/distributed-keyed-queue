package homelab.keyedqueue.domain.service.usecase.lock


import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.common.orFail
import homelab.keyedqueue.domain.request.lock.AcquireRequest
import homelab.keyedqueue.domain.response.lock.AcquireResponse
import homelab.keyedqueue.domain.model.Acquisition
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.readiness.LockReadiness
import homelab.keyedqueue.domain.types.{ LockName, Ticket }
import homelab.keyedqueue.domain.service.usecase.lock.LockAcquireUseCase.{ Waiter, Waiting }
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
        waiter   = Waiter(acquisition, asked, mailbox)
        held    <- entering(waiter, acquisition.patience)
      yield held

  /**
   * Enter for the lock, and hold the resulting ticket until it is granted, spent or lost.
   *
   * A ticket the queue has forgotten ends one attempt and starts another, with what is left of the
   * patience.
   *
   * @param waiter who is waiting, for what, and since when
   * @param within what is left of the patience, which bounds the ticket
   * @return the hold, or `None` when the patience elapsed; aborts with an `AdapterError` when the store fails
   */
  private def entering(waiter: Waiter, within: Duration): IO[ApplicationError.AdapterError, Option[LockStore.Hold]] =
    store.enter(waiter.acquisition, within).flatMap {
      case LockStore.Entered.Granted(hold)           => ZIO.some(hold)
      case LockStore.Entered.Queued(ticket, recheck) =>
        await(waiter, ticket, recheck).flatMap:
          case Waiting.Spent       => ZIO.none
          case Waiting.Taken(hold) => ZIO.some(hold)
          case Waiting.Lost        =>
            remainingTime(waiter).flatMap:
              case None       => ZIO.none
              case Some(left) => entering(waiter, left)
    }

  /**
   * Wait out one ticket, and give it up unless the wait ended holding the lock.
   *
   * A ticket lives for exactly one of these calls: its withdrawal is this call's finaliser, so whichever
   * way the wait ends — granted, patience spent, caller interrupted — the store is left holding only
   * tickets whose owners are still waiting.
   *
   * @param waiter who is waiting, for what, and since when
   * @param ticket the ticket the enter minted
   * @param recheck the first delay the enter named
   * @return how the wait ended; aborts with an `AdapterError` when the store fails
   */
  private def await(waiter: Waiter, ticket: Ticket, recheck: Duration): IO[ApplicationError.AdapterError, Waiting] =
    Clock.instant.flatMap: now =>
      ZIO
        .acquireReleaseExitWith(ZIO.succeed(ticket)) {
          case _ -> Exit.Success(Waiting.Taken(_)) => ZIO.unit
          case _                                   => store.withdraw(waiter.acquisition.name, ticket).ignore
        }(held => awaitTurn(waiter, held, now.plus(atLeastFloor(recheck))))

  /**
   * Wait for this ticket's turn, until granted, spent, or lost.
   *
   * Parks until the earlier of the next known event and the remaining patience — a wake in the mailbox cuts
   * the park short — then asks. Every ask is deliberate: it follows a wake, the arrival of the next known
   * event, or the patience running out.
   *
   * @param waiter who is waiting, for what, and since when
   * @param ticket the ticket to ask with
   * @param recheckAt when the answer can next change
   * @return how the wait ended; aborts with an `AdapterError` when the store fails
   */
  private def awaitTurn(waiter: Waiter, ticket: Ticket, recheckAt: Instant): IO[ApplicationError.AdapterError, Waiting] =
    remainingTime(waiter).flatMap:
      case None       => ZIO.succeed(Waiting.Spent)
      case Some(left) =>
        untilRecheck(recheckAt, left).flatMap: window =>
          val park = waiter.mailbox.take.timeout(window).unless(window.isZero)
          (park *> store.grant(waiter.acquisition, ticket)).flatMap:
            case LockStore.Asked.Granted(hold) => ZIO.succeed(Waiting.Taken(hold))
            case LockStore.Asked.Gone          => ZIO.succeed(Waiting.Lost)
            case LockStore.Asked.Wait(delay)   =>
              Clock.instant.flatMap(now => awaitTurn(waiter, ticket, now.plus(atLeastFloor(delay))))

  /**
   * How long to park before the next known event, bounded by the patience left.
   *
   * @param recheckAt when the answer can next change
   * @param left the patience remaining
   * @return the park; zero when the event time has already passed
   */
  private def untilRecheck(recheckAt: Instant, left: Duration): UIO[Duration] =
    Clock.instant.map: now =>
      val until = Duration.fromInterval(now, recheckAt)
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
   * What is left of this waiter's patience.
   *
   * @param waiter who is waiting, for what, and since when
   * @return the time still to wait, or `None` when it is spent
   */
  private def remainingTime(waiter: Waiter): UIO[Option[Duration]] =
    Clock.instant.map: now =>
      val left = waiter.acquisition.patience.minus(Duration.fromInterval(waiter.asked, now))
      Option.when(left.toMillis > 0)(left)


object LockAcquireUseCase:

  /** The least a waiter parks between grant attempts, so clock-boundary refusals cannot spin. */
  private val floor: Duration = 10.millis

  /**
   * One caller's wait: what it asked for, when it asked, and where its wakes land.
   *
   * None of the three changes while the wait lasts, which is what separates them from the ticket and the
   * recheck time the loop carries.
   *
   * @param acquisition the lock to take, how long to hold it, and how long to wait
   * @param asked when the call arrived, which the patience is measured from
   * @param mailbox where this waiter's wakes land
   */
  private case class Waiter(acquisition: Acquisition, asked: Instant, mailbox: Queue[Unit])

  /**
   * How one ticket's wait ended.
   */
  private enum Waiting:

    /**
     * The turn came.
     *
     * @param hold what authorises releasing and refreshing the lock
     */
    case Taken(hold: LockStore.Hold)

    /** The patience ran out while this ticket still waited. */
    case Spent

    /** The queue no longer knows this ticket, so the caller must enter again. */
    case Lost
