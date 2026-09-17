package homelab.keyedqueue.domain.service.usecase.lock


import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import homelab.common.orFail
import homelab.keyedqueue.domain.model.Acquisition
import homelab.keyedqueue.domain.request.lock.AcquireRequest
import homelab.keyedqueue.domain.response.lock.AcquireResponse
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.readiness.LockReadiness
import homelab.keyedqueue.domain.service.readiness.LockReadiness.Signal
import homelab.keyedqueue.domain.service.usecase.lock.LockAcquireUseCase.{ Waiter, atLeastFloor, window }
import homelab.keyedqueue.domain.service.validation.LockInputValidation
import homelab.keyedqueue.domain.types.Ticket
import zio.*

import java.time.Instant


/**
 * Take a lock, waiting until it is free or the caller's patience runs out.
 *
 * A wait is a small machine — see [[AcquireLifecycle]]: ask for the lock, and while somebody else holds it,
 * hold a ticket and park until the event the store named or a release wakes this waiter. The ticket is
 * given up on every exit that is not a grant.
 *
 * @param store where the lock lives
 * @param validation what turns a request into an acquisition this service will honour
 * @param readiness where a waiter parks, and what a release wakes
 */
final class LockAcquireUseCase(store: LockStore, validation: LockInputValidation, readiness: LockReadiness):

  /**
   * Parse, then acquire.
   *
   * @param request the lock and what the caller is asking for, untrusted
   * @return the grant, or `Unavailable` when the wait elapsed; aborts with `ValidationError` when the
   *         request is malformed, or with `ApplicationError` when the store fails
   */
  def apply(request: AcquireRequest): IO[ApplicationError, AcquireResponse] =
    ZIO.scoped:
      for
        acquisition <- validation.parse(request).orFail
        asked       <- Clock.instant
        waiter      <- readiness.subscribe(acquisition.name).map(Waiter(acquisition, asked, _))
        result      <- AcquireLifecycle.run(waiter, acquisition.patience)
      yield result match
        case Some(hold) => AcquireResponse.Granted(hold.claim, hold.leaseUntil)
        case None       => AcquireResponse.Unavailable

  /**
   * Where one caller's wait has got to, and how it advances from there.
   *
   * Two states are live and carry a transition of their own;
   * the other two answer the caller and stay as they are.
   */
  sealed private trait AcquireLifecycle:

    /**
     * Advance this wait by one step.
     *
     * @return the state the step leaves the wait in; aborts with an `AdapterError` when the store fails
     */
    def next: IO[AdapterError, AcquireLifecycle] = ZIO.succeed(this)

  /**
   * The states a wait passes through, and the driver that runs them.
   *
   * [[run]] starts at [[Placing]] and follows each state's own transition until one of the two that answer.
   */
  private object AcquireLifecycle {

    /** The patience ran out before a turn came. */
    case object GivenUp extends AcquireLifecycle

    /**
     * The lock is now held by the caller.
     *
     * @param hold what authorises releasing and refreshing it
     */
    case class Granted(hold: LockStore.Hold) extends AcquireLifecycle

    /**
     * About to ask for the lock, with no place in the acquisition queue yet.
     *
     * @param waiter who is waiting, for what, and since when
     * @param within what is left of the patience, which bounds any ticket the ask is answered with
     */
    class Placing(waiter: Waiter, within: Duration) extends AcquireLifecycle:

      /**
       * Ask for the lock: it is granted, or this caller takes a place in the queue.
       *
       * @return the lock, or a place in the queue with the first recheck time; aborts with an
       *         `AdapterError` when the store fails
       */
      override def next: IO[AdapterError, AcquireLifecycle] =
        for
          position <- store.place(waiter.acquisition, within)
          now      <- Clock.instant
        yield position match
          case LockStore.Position.Granted(hold)           => Granted(hold)
          case LockStore.Position.Queued(ticket, recheck) => Queued(waiter, ticket, now.plus(atLeastFloor(recheck)))

    /**
     * Holding a place in the queue, with the next ask due at a known time.
     *
     * @param waiter who is waiting, for what, and since when
     * @param ticket this caller's place
     * @param recheckAt when the answer can next change
     */
    class Queued(waiter: Waiter, ticket: Ticket, recheckAt: Instant) extends AcquireLifecycle:

      /**
       * Park until the next event, then ask whether it is this ticket's turn.
       *
       * The ticket is given up on every exit but two — staying queued, and being granted — so the patience
       * running out, a failure and an interruption all return this caller's place to the queue.
       *
       * @return the lock, a later recheck, a fresh start when the queue no longer knows this ticket, or the
       *         end of the wait; aborts with an `AdapterError` when the store fails
       */
      override def next: IO[AdapterError, AcquireLifecycle] =
        remainingTime
          .flatMap:
            case None                => ZIO.succeed(GivenUp)
            case Some(remainingTime) =>
              awaitTurn(remainingTime).map:
                case _ -> LockStore.Turn.Gone          => Placing(waiter, remainingTime)
                case _ -> LockStore.Turn.Granted(hold) => Granted(hold)
                case now -> LockStore.Turn.Wait(delay) => Queued(waiter, ticket, now.plus(atLeastFloor(delay)))
          .onExit:
            case Exit.Success(_: Queued)  => ZIO.unit
            case Exit.Success(_: Granted) => ZIO.unit
            case _                        => store.withdraw(waiter.acquisition.name, ticket).ignore

      /**
       * What is left of this waiter's patience.
       *
       * @return the time still to wait, or `None` when it is spent
       */
      private def remainingTime: UIO[Option[Duration]] =
        Clock.instant.map: now =>
          val elapsed = Duration.fromInterval(waiter.asked, now)
          val left    = waiter.acquisition.patience.minus(elapsed)
          Option.when(left.toMillis > 0)(left)

      /**
       * Park until the next event or a wake, then ask whether it is this ticket's turn.
       *
       * The clock is read twice: once to size the park, and once at the ask, which is the instant a `Wait`
       * delay counts from.
       *
       * @param left the patience remaining, which bounds the park
       * @return when the ask was made, and what the store answered; aborts with an `AdapterError` when the
       *         store fails
       */
      private def awaitTurn(left: Duration): IO[AdapterError, (Instant, LockStore.Turn)] =
        for
          timeout <- Clock.instant.map(window(_, recheckAt, left))
          _       <- waiter.signal.await.timeout(timeout).unless(timeout.isZero)
          asking  <- Clock.instant
          answer  <- store.ask(waiter.acquisition, ticket)
        yield asking -> answer

    /**
     * Wait for the lock from the first ask until it is held or the patience is spent.
     *
     * @param waiter who is waiting, for what, and since when
     * @param within the patience the first ask may claim a ticket for
     * @return the hold, or `None` when the patience elapsed first; aborts with an `AdapterError` when the
     *         store fails
     */
    def run(waiter: Waiter, within: Duration): IO[AdapterError, Option[LockStore.Hold]] =
      loop(Placing(waiter, within))

    /**
     * Advance the wait until it reaches a state that answers.
     *
     * A step is interruptible — a waiter parks inside one — and the space between two steps is not, so a
     * step's own handlers are what decide the fate of anything that step holds.
     *
     * @param state where the wait has got to
     * @return the hold, or `None` when the wait gave up; aborts with an `AdapterError` when the store fails
     */
    private def loop(state: AcquireLifecycle): IO[AdapterError, Option[LockStore.Hold]] =
      ZIO.uninterruptibleMask: restore =>
        restore(state.next).flatMap:
          case Granted(hold) => ZIO.succeed(Some(hold))
          case GivenUp       => ZIO.succeed(None)
          case next          => restore(loop(next))
  }


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
   * @param signal what a wake on this lock reaches
   */
  private case class Waiter(acquisition: Acquisition, asked: Instant, signal: Signal)

  /**
   * How long to park before the next event, bounded by the patience left.
   *
   * @param now       when the park starts
   * @param recheckAt when the answer can next change
   * @param left      the patience remaining
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
    if delay.toMillis < floor.toMillis
    then floor
    else delay
