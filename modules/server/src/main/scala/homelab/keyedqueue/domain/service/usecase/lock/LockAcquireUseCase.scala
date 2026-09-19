package homelab.keyedqueue.domain.service.usecase.lock


import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import homelab.common.flow.Recursion
import homelab.common.orFail
import homelab.keyedqueue.domain.model.lock.{ Demand, Hold, Position, Turn }
import homelab.keyedqueue.domain.request.lock.AcquireRequest
import homelab.keyedqueue.domain.response.lock.AcquireResponse
import homelab.keyedqueue.domain.service.persistence.LockStore
import homelab.keyedqueue.domain.service.readiness.LockReadiness
import homelab.keyedqueue.domain.service.readiness.LockReadiness.Signal
import homelab.keyedqueue.domain.service.usecase.lock.LockAcquireUseCase.AcquireLifecycle.*
import homelab.keyedqueue.domain.service.usecase.lock.LockAcquireUseCase.{ AcquireLifecycle, Waiter, atLeastFloor }
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
 * @param validation what turns a request into an demand this service will honour
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
        demand <- validation.parse(request).orFail
        asked  <- Clock.instant
        signal <- readiness.subscribe(demand.name)
        waiter  = Waiter(demand, asked, signal)
        result <- acquire(waiter, demand.patience)
      yield result match
        case Some(hold) => AcquireResponse.Granted(hold, demand)
        case None       => AcquireResponse.Unavailable

  /**
   * Follow one caller's wait from its first ask until the lock is held or the patience is spent.
   *
   * A step is interruptible — a waiter parks inside one — and the space between two steps is not, so a
   * step's own handlers are what decide the fate of the ticket it holds.
   *
   * @param waiter who is waiting, for what, and since when
   * @param patience the wait this caller asked for, which the first ask may claim a ticket for
   * @return the hold, or `None` when the patience elapsed first; aborts with an `AdapterError` when the
   *         store fails
   */
  private def acquire(waiter: Waiter, patience: Duration): IO[AdapterError, Option[Hold]] =
    Recursion(Demanding(waiter, patience)) {
      case state: Demanding => placing(state)
      case state: Queued    => queuing(state)
      case state            => ZIO.succeed(state)
    }.terminate {
      case GivenUp       => None
      case Granted(hold) => Some(hold)
    }

  /**
   * Ask for the lock: it is granted, or this caller takes a place in the queue.
   *
   * The step is uninterruptible, so a place that reached the store is always observed and its ticket
   * arrives at the state that withdraws it. The clock is read before the ask, which is the instant a
   * recheck delay counts from.
   *
   * @param state the lock to take, and what is left of the patience to claim a ticket for
   * @return the lock, or a place in the queue with the first recheck time; aborts with an `AdapterError`
   *         when the store fails
   */
  private def placing(state: Demanding): IO[AdapterError, AcquireLifecycle] = ZIO.uninterruptible {
    for
      now      <- Clock.instant
      demand    = state.waiter.demand
      position <- store.place(demand.name, demand.ttl, state.within)
    yield position match
      case Position.Granted(hold)           => Granted(hold)
      case Position.Queued(ticket, recheck) => Queued(state.waiter, ticket, now.plus(atLeastFloor(recheck)))
  }

  /**
   * Park until the next event, then ask whether it is this ticket's turn.
   *
   * The ticket is given up on every exit but two — staying queued, and being granted — so the patience
   * running out, a failure and an interruption all return this caller's place to the queue.
   *
   * @param state this caller's place in the queue, and when its answer can next change
   * @return the lock, a later recheck, a fresh start when the queue no longer knows this ticket, or the
   *         end of the wait; aborts with an `AdapterError` when the store fails
   */
  private def queuing(state: Queued): IO[AdapterError, AcquireLifecycle] =
    patienceLeft(state)
      .flatMap:
        case None           => ZIO.succeed(GivenUp)
        case Some(patience) => awaitTurn(state, patience)
      .onExit:
        case Exit.Success(_: Queued)  => ZIO.unit
        case Exit.Success(_: Granted) => ZIO.unit
        case _                        => store.withdraw(state.waiter.demand.name, state.ticket).ignore

  /**
   * What is left of this waiter's patience.
   *
   * @param state the wait to measure, which carries when it was asked for
   * @return the time still to wait, or `None` when it is spent
   */
  private def patienceLeft(state: Queued): UIO[Option[Duration]] =
    Clock.instant.map: now =>
      val elapsed = Duration.fromInterval(state.waiter.asked, now)
      val left    = state.waiter.demand.patience.minus(elapsed)
      Option.when(left > Duration.Zero)(left)

  /**
   * Park until the next event or a wake, then ask whether it is this ticket's turn.
   *
   * The clock is read twice: once to size the park, and once at the ask, which is the instant a `Wait`
   * delay counts from.
   *
   * @param state this caller's place in the queue, and when its answer can next change
   * @param patience the patience remaining, which bounds the park
   * @return the state the ask leaves the wait in: the lock, a later recheck, or a fresh start when the
   *         queue no longer knows this ticket; aborts with an `AdapterError` when the store fails
   */
  private def awaitTurn(state: Queued, patience: Duration): IO[AdapterError, AcquireLifecycle] =
    for
      waiter   = state.waiter
      demand   = waiter.demand
      // The shorter of the patience left and the next recheck from now.
      timeout <- Clock.instant.map(now => Duration.fromInterval(now, state.recheckAt) min patience)
      _       <- waiter.signal.await.timeout(timeout).unless(timeout <= Duration.Zero)
      now     <- Clock.instant
      answer  <- store.ask(demand.name, demand.ttl, state.ticket)
    yield answer match
      case Turn.Gone          => Demanding(waiter, patience)
      case Turn.Granted(hold) => Granted(hold)
      case Turn.Wait(delay)   => Queued(waiter, state.ticket, now.plus(atLeastFloor(delay)))


object LockAcquireUseCase:

  /** The least a waiter parks between grant attempts, so clock-boundary refusals cannot spin. */
  private val floor: Duration = 10.millis

  /**
   * One caller's wait: what it asked for, when it asked, and where its wakes land.
   *
   * None of the three changes while the wait lasts.
   *
   * @param demand the lock to take, how long to hold it, and how long to wait
   * @param asked when the call arrived, which the patience is measured from
   * @param signal what a wake on this lock reaches
   */
  private[lock] case class Waiter(demand: Demand, asked: Instant, signal: Signal)

  /**
   * Where one caller's wait has got to.
   *
   * Two of these carry a wait that is still going, and two are answers. Which of them ends a run is the
   * caller's question rather than a property of the four — see [[acquire]].
   */
  private[lock] enum AcquireLifecycle:

    /** The patience ran out before a turn came. */
    case GivenUp

    /**
     * The lock is now held by this caller.
     *
     * @param hold what authorises releasing and refreshing it
     */
    case Granted(hold: Hold)

    /**
     * About to ask for the lock, with no place in the queue yet.
     *
     * @param waiter who is waiting, for what, and since when
     * @param within what is left of the patience, which bounds any ticket the ask is answered with
     */
    case Demanding(waiter: Waiter, within: Duration)

    /**
     * Holding a place in the queue, with the next ask due at a known time.
     *
     * @param waiter who is waiting, for what, and since when
     * @param ticket this caller's place
     * @param recheckAt when the answer can next change
     */
    case Queued(waiter: Waiter, ticket: Ticket, recheckAt: Instant)

  /**
   * A delay no shorter than the spin floor.
   *
   * @param delay what the store named
   * @return that, or the floor, whichever is longer
   */
  private def atLeastFloor(delay: Duration): Duration =
    if delay.toMillis < floor.toMillis then floor else delay
