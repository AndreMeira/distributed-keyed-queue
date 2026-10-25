package homelab.keyedqueue.domain.service.usecase.queue


import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import homelab.common.flow.Recursion
import homelab.common.orFail
import homelab.keyedqueue.domain.model.queue.{ Demand, Grant, Settlement }
import homelab.keyedqueue.domain.model.queue.Settlement.{ Outcome, Verdict }
import homelab.keyedqueue.domain.request.queue.DequeueRequest
import homelab.keyedqueue.domain.response.queue.*
import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.readiness.QueueReadiness
import homelab.keyedqueue.domain.service.readiness.QueueReadiness.Signal
import homelab.keyedqueue.domain.service.usecase.queue.DequeueUseCase.DequeueLifecycle.*
import homelab.keyedqueue.domain.service.usecase.queue.DequeueUseCase.DequeueLifecycle
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import zio.*

import java.time.Instant


/**
 * Wait for work on a queue, and hand it to the caller with the lease that comes with it.
 *
 * A wait is a small machine — see [[DequeueLifecycle]]: take the queue's readiness token, look, and leave
 * with what the look found or park for the next token. The claim happens in the caller's own fiber: the
 * fiber that waits is the fiber that receives, so no work is ever claimed for a caller that has since gone.
 *
 * @param store where the queue lives
 * @param watchdog told about the queue, so its abandoned work is repaired
 * @param validation what turns a request into a demand this service will honour
 * @param readiness where a consumer parks, and what an announcement wakes
 */
final class DequeueUseCase(
  store: QueueStore,
  watchdog: Watchdog,
  validation: QueueInputValidation,
  readiness: QueueReadiness,
):

  /**
   * Wait for a message.
   *
   * A caller asking to wait longer than the service allows is clamped rather than refused: patience is a
   * preference, not a mistake, and the response says when the wait actually ended. The same for asking for
   * a bigger batch than the service will hand over — the response says how many came back, and
   * `backlogDepth` says how many were left. Both clamps happen in the parse, which is what makes a
   * `Demand` mean "within this service's limits" wherever one turns up.
   *
   * The call is a scope, and a claim is acquired in it: a call that does not answer after its claim was made,
   * because it was cancelled, hands the claim back rather than leaving it claimed until its lease expires. Its
   * messages are claimable again at once and count one more delivery attempt. A call cancelled after it has
   * answered is beyond this: the response is lost on its way out, and the claim lapses on its lease.
   *
   * @param request the queue and what the caller is asking for, untrusted
   * @return the delivery, or nothing when none became ready in time; aborts with `ValidationError` when the
   *         queue is unnamed or the batch is negative, or with `ApplicationError` when the store fails
   */
  def apply(request: DequeueRequest): IO[ApplicationError, DequeueResponse] =
    ZIO.scoped:
      for
        demand <- validation.parse(request).orFail
        _      <- watchdog.watch(demand.queue)
        asked  <- Clock.instant
        signal <- readiness.subscribe(demand.queue)
        grant  <- claim(demand, asked, signal)
      yield response(grant)

  /**
   * Follow one caller's wait from its first look until it has work or the patience is spent.
   *
   * The one live state takes its token and looks inside a single step. A token is a resource with no
   * deadline of its own, and [[Recursion]] guarantees nothing about what crosses a step boundary, so a token
   * never does.
   *
   * @param demand the queue, how many to take, and how long to wait
   * @param asked when the call arrived, which the patience is measured from
   * @param signal what an announcement on this queue reaches
   * @return the grant, or `None` when the patience elapsed first; aborts with an `AdapterError` when the
   *         store fails; a grant is acquired in the scope, see [[acquireClaim]]
   */
  private def claim(demand: Demand, asked: Instant, signal: Signal): ZIO[Scope, AdapterError, Option[Grant]] =
    Recursion(Waiting(demand, asked, signal)) {
      case state: Waiting => next(state)
      case state          => ZIO.succeed(state)
    }.terminate {
      case GivenUp        => None
      case Granted(grant) => Some(grant)
    }

  /**
   * Continue one caller's wait, or end it when the patience is spent.
   *
   * @param state the wait to continue
   * @return the grant, the same wait to continue, or the end of the patience; aborts with an `AdapterError`
   *         when the store fails
   */
  private def next(state: Waiting): ZIO[Scope, AdapterError, DequeueLifecycle] =
    patienceLeft(state).flatMap:
      case None       => ZIO.succeed(GivenUp)
      case Some(left) => awaitWork(state, left)

  /**
   * What is left of this waiter's patience.
   *
   * @param state the wait to measure, which carries when it was asked for
   * @return the time still to wait, or `None` when it is spent
   */
  private def patienceLeft(state: Waiting): UIO[Option[Duration]] =
    Clock.instant.map: now =>
      val left = state.demand.patience.minus(Duration.fromInterval(state.asked, now))
      Option.when(left.toMillis > 0)(left)

  /**
   * Take a token, look, and decide what becomes of the token.
   *
   * Only the park is interruptible; the look is an acquisition, so it always finishes. A look that finds work
   * hands the token on through [[QueueReadiness.ready]], so the next consumer looks too. A look that finds
   * nothing keeps it, which is what ends the chain. A look that fails hands it on as well, since it cannot know
   * what it would have found.
   *
   * @param state the wait to continue: who is waiting, for what, and since when
   * @param patience what is left of the wait, which the park is bounded by
   * @return the grant, the same wait to continue, or the end of the patience when no token came in time;
   *         aborts with an `AdapterError` when the store fails
   */
  private def awaitWork(state: Waiting, patience: Duration): ZIO[Scope, AdapterError, DequeueLifecycle] =
    ZIO.uninterruptibleMask { restore =>
      restore(state.signal.await(patience)).flatMap:
        case false => ZIO.succeed(GivenUp)
        case true  =>
          acquireClaim(state.demand)
            .map:
              case Some(grant) => Granted(grant)
              case None        => state
            .onExit:
              case Exit.Success(_: Waiting) => ZIO.unit
              case _                        => readiness.ready(state.demand.queue)
    }

  /**
   * Claim from the queue, as a resource of the call's scope: when the call ends without answering, the claim
   * is handed back.
   *
   * @param demand the queue and how many to take
   * @return the claim, or `None` when nothing was claimable; aborts with an `AdapterError` when the store fails
   */
  private def acquireClaim(demand: Demand): ZIO[Scope, AdapterError, Option[Grant]] =
    ZIO.acquireReleaseExit(store.attemptClaim(demand.queue, demand.batch))(handBackUnanswered)

  /**
   * Hand back a claim whose call did not answer.
   *
   * @param claimed what the claim found
   * @param ended how the call's scope closed
   * @return noop; never fails
   */
  private def handBackUnanswered(claimed: Option[Grant], ended: Exit[Any, Any]): UIO[Unit] =
    claimed match
      case Some(grant) if !ended.isSuccess => handBack(grant)
      case _                               => ZIO.unit

  /**
   * Return every message of a grant to its key, claimable again at once. A settle that does not land is
   * left alone: the claim then lapses on its lease, as it would for a consumer that died.
   *
   * @param grant what was claimed for a call that did not complete
   * @return noop; never fails
   */
  private def handBack(grant: Grant): UIO[Unit] =
    store
      .settle(Settlement(grant.claim, grant.messages.map(owned => Outcome(owned.id, Verdict.Failed)), None))
      .ignore

  /**
   * Present what the store returned as the answer the caller gets.
   *
   * @param grant what the store handed over, or nothing when the wait elapsed first
   * @return the response, carrying a claim only when there was one
   */
  private def response(grant: Option[Grant]): DequeueResponse =
    grant match
      case None          => DequeueResponse.Empty
      case Some(granted) => DequeueResponse.fromGrant(granted)


object DequeueUseCase:

  /**
   * Where one caller's wait has got to.
   *
   * One of these carries a wait that is still going, and two are answers. Which of them ends a run is the
   * caller's question rather than a property of the three — see [[claim]].
   */
  private[queue] enum DequeueLifecycle:

    /** The patience ran out before a look found anything. */
    case GivenUp

    /**
     * A look found work, which is now the caller's.
     *
     * @param grant what was claimed, and the lease that came with it
     */
    case Granted(grant: Grant)

    /**
     * About to take a token and look. None of the three changes while the wait lasts.
     *
     * @param demand the queue, how many to take, and how long to wait
     * @param asked when the call arrived, which the patience is measured from
     * @param signal what an announcement on this queue reaches
     */
    case Waiting(demand: Demand, asked: Instant, signal: Signal)
