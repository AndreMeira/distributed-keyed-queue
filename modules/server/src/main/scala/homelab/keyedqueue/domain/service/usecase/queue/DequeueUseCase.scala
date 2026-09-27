package homelab.keyedqueue.domain.service.usecase.queue


import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import homelab.common.flow.Recursion
import homelab.common.orFail
import homelab.keyedqueue.domain.model.queue.{ Demand, Grant }
import homelab.keyedqueue.domain.request.queue.DequeueRequest
import homelab.keyedqueue.domain.response.queue.*
import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.readiness.QueueReadiness
import homelab.keyedqueue.domain.service.readiness.QueueReadiness.Signal
import homelab.keyedqueue.domain.service.usecase.queue.DequeueUseCase.DequeueLifecycle.*
import homelab.keyedqueue.domain.service.usecase.queue.DequeueUseCase.{ DequeueLifecycle, Waiter }
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
   * @param request the queue and what the caller is asking for, untrusted
   * @return the delivery, or nothing when none became ready in time; aborts with `ValidationError` when the
   *         queue is unnamed or the batch is negative, or with `ApplicationError` when the store fails
   */
  def apply(request: DequeueRequest): IO[ApplicationError, DequeueResponse] =
    for
      demand <- validation.parse(request).orFail
      _      <- watchdog.watch(demand.queue)
      asked  <- Clock.instant
      signal <- readiness.subscribe(demand.queue)
      grant  <- claim(Waiter(demand, asked, signal))
    yield response(grant)

  /**
   * Follow one caller's wait from its first look until it has work or the patience is spent.
   *
   * The one live state takes its token and looks inside a single step. A token is a resource with no
   * deadline of its own, and [[Recursion]] guarantees nothing about what crosses a step boundary, so a token
   * never does.
   *
   * @param waiter who is waiting, for what, and since when
   * @return the grant, or `None` when the patience elapsed first; aborts with an `AdapterError` when the
   *         store fails
   */
  private def claim(waiter: Waiter): IO[AdapterError, Option[Grant]] =
    Recursion(Waiting(waiter)) {
      case state: Waiting => looking(state)
      case state          => ZIO.succeed(state)
    }.terminate {
      case GivenUp        => None
      case Granted(grant) => Some(grant)
    }

  /**
   * Take a token, look, and decide what becomes of the token.
   *
   * The park and the look are interruptible and the space around them is not, so a token that was taken is
   * always disposed of by the handler here. A look that finds work hands the token on through
   * [[QueueReadiness.ready]], so the next consumer looks too. A look that finds nothing keeps it, which is
   * what ends the chain. A look that does not finish — a failure, an interruption — hands it on as well,
   * since it cannot know what it would have found.
   *
   * @param state the wait to continue
   * @return the grant, the same wait to continue, or the end of the patience; aborts with an `AdapterError`
   *         when the store fails
   */
  private def looking(state: Waiting): IO[AdapterError, DequeueLifecycle] =
    val waiter = state.waiter
    val demand = waiter.demand
    patienceLeft(waiter).flatMap:
      case None       => ZIO.succeed(GivenUp)
      case Some(left) =>
        ZIO.uninterruptibleMask: restore =>
          restore(waiter.signal.await(left)).flatMap:
            case false => ZIO.succeed(Waiting(waiter))
            case true  =>
              restore(store.attemptClaim(demand.queue, demand.batch))
                .map {
                  case Some(grant) => Granted(grant)
                  case None        => Waiting(waiter)
                }
                .onExit {
                  case Exit.Success(_: Waiting) => ZIO.unit
                  case _                        => readiness.ready(demand.queue)
                }

  /**
   * What is left of this waiter's patience.
   *
   * @param waiter the wait to measure, which carries when it was asked for
   * @return the time still to wait, or `None` when it is spent
   */
  private def patienceLeft(waiter: Waiter): UIO[Option[Duration]] =
    Clock.instant.map: now =>
      val left = waiter.demand.patience.minus(Duration.fromInterval(waiter.asked, now))
      Option.when(left.toMillis > 0)(left)

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
   * One caller's wait: what it asked for, when it asked, and where its tokens come from.
   *
   * None of the three changes while the wait lasts.
   *
   * @param demand the queue, how many to take, and how long to wait
   * @param asked when the call arrived, which the patience is measured from
   * @param signal what an announcement on this queue reaches
   */
  private[queue] case class Waiter(demand: Demand, asked: Instant, signal: Signal)

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
     * About to take a token and look.
     *
     * @param waiter who is waiting, for what, and since when
     */
    case Waiting(waiter: Waiter)
