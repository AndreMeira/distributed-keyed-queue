package homelab.keyedqueue.domain.service.usecase.lock


import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.common.error.ApplicationError.AdapterError
import homelab.common.orFail
import homelab.keyedqueue.domain.model.Acquisition
import homelab.keyedqueue.domain.request.lock.AcquireRequest
import homelab.keyedqueue.domain.response.lock.AcquireResponse
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.readiness.LockReadiness
import homelab.keyedqueue.domain.service.usecase.lock.LockAcquireUseCase.{ State, Waiter }
import homelab.keyedqueue.domain.service.validation.LockInputValidation
import homelab.keyedqueue.domain.types.Ticket
import zio.*

import java.time.Instant


/**
 * Take a lock, waiting until it is free or the caller's patience runs out.
 *
 * A wait is a small machine over [[State]]: ask for the lock, and while somebody else holds it, hold a
 * ticket and park until the event the store named or a release wakes this waiter. The ticket is given up on
 * every exit that is not a grant.
 *
 * @param store where the lock lives
 * @param validation what turns a request into an acquisition this service will honour
 * @param readiness where a waiter parks, and what a release wakes
 */
final class LockAcquireUseCase(store: LockStore, validation: LockInputValidation, readiness: LockReadiness):

  /**
   * Parse, then acquire.
   *
   * Nothing acquired in time is a response, not a failure: a caller that waited its patience for a held
   * lock has behaved exactly as asked.
   *
   * @param request the lock and what the caller is asking for, untrusted
   * @return the grant, or `Unavailable` when the wait elapsed; aborts with `ValidationError` when the
   *         request is malformed, or with `ApplicationError` when the store fails
   */
  def apply(request: AcquireRequest): IO[ApplicationError, AcquireResponse] =
    validation.parse(request).orFail.flatMap(acquire).map {
      case Some(hold) => AcquireResponse.Granted(hold.claim, hold.leaseUntil)
      case None       => AcquireResponse.Unavailable
    }

  /**
   * Run the wait from the first ask until the lock is held or the patience is spent.
   *
   * The mailbox is subscribed before the first ask, so a release cannot land in the gap between asking and
   * parking.
   *
   * @param acquisition the lock to take, how long to hold it, and how long to wait
   * @return the hold, or `None` when the patience elapsed first; aborts with an `AdapterError` when the
   *         store fails
   */
  private def acquire(acquisition: Acquisition): IO[AdapterError, Option[LockStore.Hold]] =
    ZIO.scoped:
      for
        asked   <- Clock.instant
        mailbox <- readiness.subscribe(acquisition.name)
        waiter   = Waiter(acquisition, asked, mailbox)
        held    <- loop(State.Entering(acquisition.patience)):
                     case State.Entering(within)          => entering(waiter, within)
                     case State.Queued(ticket, recheckAt) => queued(waiter, ticket, recheckAt)
      yield held

  /**
   * Run the machine until a step answers.
   *
   * A step is interruptible — a waiter parks inside one — and the space between two steps is not, so a
   * step's own handlers are what decide the fate of anything that step holds. See `homelab-toolkit-zio`'s
   * `docs/research/scoped-loop.md`.
   *
   * @param state where the machine is
   * @param run one transition
   * @return the hold, or `None` when the machine gave up; aborts with an `AdapterError` when the store fails
   */
  private def loop(
    state: State.Entering | State.Queued
  )(
    run: State.Entering | State.Queued => IO[AdapterError, State]
  ): IO[AdapterError, Option[LockStore.Hold]] =
    ZIO.uninterruptibleMask: restore =>
      restore(run(state)).flatMap:
        case State.Granted(hold)  => ZIO.succeed(Some(hold))
        case State.GivenUp        => ZIO.succeed(None)
        case next: State.Queued   => restore(loop(next)(run))
        case next: State.Entering => restore(loop(next)(run))

  /**
   * Ask for the lock: it is granted, or this caller takes a place in the queue.
   *
   * @param waiter who is waiting, for what, and since when
   * @param within what is left of the patience, which bounds the ticket this may be given
   * @return where the answer leaves the wait; aborts with an `AdapterError` when the store fails
   */
  private def entering(waiter: Waiter, within: Duration): IO[AdapterError, State] =
    Clock.instant.flatMap: now =>
      store.enter(waiter.acquisition, within).map {
        case LockStore.Entered.Granted(hold)           => State.Granted(hold)
        case LockStore.Entered.Queued(ticket, recheck) => State.Queued(ticket, now.plus(atLeastFloor(recheck)))
      }

  /**
   * Park until the next event, ask, and say where that leaves the wait.
   *
   * The ticket is given up on every exit but two — staying queued, and being granted — so the patience
   * running out, a failure and an interruption all return this caller's place to the queue.
   *
   * @param waiter who is waiting, for what, and since when
   * @param ticket this caller's place in the queue
   * @param recheckAt when the answer can next change
   * @return where the answer leaves the wait; aborts with an `AdapterError` when the store fails
   */
  private def queued(waiter: Waiter, ticket: Ticket, recheckAt: Instant): IO[AdapterError, State] =
    remainingTime(waiter)
      .flatMap:
        case None       => ZIO.succeed(State.GivenUp)
        case Some(left) =>
          askGrant(waiter, ticket, recheckAt, left).map:
            case _ -> LockStore.Asked.Gone          => State.Entering(left)
            case _ -> LockStore.Asked.Granted(hold) => State.Granted(hold)
            case now -> LockStore.Asked.Wait(delay) => State.Queued(ticket, now.plus(atLeastFloor(delay)))
      .onExit:
        case Exit.Success(State.Queued(_, _)) => ZIO.unit
        case Exit.Success(State.Granted(_))   => ZIO.unit
        case _                                => store.withdraw(waiter.acquisition.name, ticket).ignore

  /**
   * Park until the next event or a wake, then ask whether it is this ticket's turn.
   *
   * The clock is read twice: once to size the park, and once at the ask, which is the instant a `Wait`
   * delay counts from.
   *
   * @param waiter who is waiting, for what, and since when
   * @param ticket the ticket to ask with
   * @param recheckAt when the answer can next change
   * @param left the patience remaining
   * @return when the ask was made, and what the store answered; aborts with an `AdapterError` when the
   *         store fails
   */
  private def askGrant(
    waiter: Waiter,
    ticket: Ticket,
    recheckAt: Instant,
    left: Duration,
  ): IO[AdapterError, (Instant, LockStore.Asked)] =
    for
      parking <- Clock.instant
      timeout  = window(parking, recheckAt, left)
      _       <- waiter.mailbox.take.timeout(timeout).unless(timeout.isZero)
      asking  <- Clock.instant
      answer  <- store.grant(waiter.acquisition, ticket)
    yield asking -> answer

  /**
   * How long to park before the next event, bounded by the patience left.
   *
   * @param now when the park starts
   * @param recheckAt when the answer can next change
   * @param left the patience remaining
   * @return the park; zero when the event time has already passed
   */
  private def window(now: Instant, recheckAt: Instant, left: Duration): Duration =
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
    if delay.toMillis < LockAcquireUseCase.floor.toMillis
    then LockAcquireUseCase.floor
    else delay

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
   * None of the three changes while the wait lasts.
   *
   * @param acquisition the lock to take, how long to hold it, and how long to wait
   * @param asked when the call arrived, which the patience is measured from
   * @param mailbox where this waiter's wakes land
   */
  private case class Waiter(acquisition: Acquisition, asked: Instant, mailbox: Queue[Unit])

  /**
   * Where a wait has got to.
   *
   * The first two are live and a step starts from one of them; the last two answer the caller.
   */
  private enum State:

    /**
     * About to ask for the lock.
     *
     * @param within what is left of the patience, which bounds any ticket this asks for
     */
    case Entering(within: Duration)

    /**
     * Holding a place in the queue.
     *
     * @param ticket this caller's place
     * @param recheckAt when the answer can next change
     */
    case Queued(ticket: Ticket, recheckAt: Instant)

    /**
     * The lock is this caller's.
     *
     * @param hold what authorises releasing and refreshing it
     */
    case Granted(hold: LockStore.Hold)

    /** The patience ran out. */
    case GivenUp
