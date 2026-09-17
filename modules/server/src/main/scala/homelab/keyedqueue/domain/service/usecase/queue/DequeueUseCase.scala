package homelab.keyedqueue.domain.service.usecase.queue


import homelab.common.error.ApplicationError
import homelab.common.orFail
import homelab.keyedqueue.domain.model.{ Demand, Grant }
import homelab.keyedqueue.domain.request.queue.DequeueRequest
import homelab.keyedqueue.domain.response.queue.*
import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.readiness.QueueReadiness
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import zio.{ Clock, Duration, IO, UIO, ZIO }

import java.time.Instant


/**
 * Wait for work on a queue, and hand it to the caller with the lease that comes with it.
 *
 * The claim happens in the caller's own fiber: the fiber that waits is the fiber that receives, so no work
 * is ever claimed for a caller that has since gone.
 *
 * @param store where the queue lives
 * @param watchdog told about the queue, so its abandoned work is repaired
 * @param validation what turns a request into a demand this service will honour
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
      now    <- Clock.instant
      grant  <- claim(demand, now)
    yield response(grant)

  /**
   * Wait for a readiness token, claim when one arrives, and keep at it until the patience is spent.
   *
   * @param demand what the caller asked for
   * @param asked  when its call arrived, which is what the patience is measured from
   * @return the claim, or `None` when the patience elapsed; aborts with an `AdapterError` when the store fails
   */
  private def claim(demand: Demand, asked: Instant): IO[ApplicationError.AdapterError, Option[Grant]] =
    remainingTime(demand.patience, asked).flatMap:
      case None               => ZIO.none
      case Some(patienceLeft) =>
        readiness
          .awaitReady(demand.queue, patienceLeft):
            store.attemptClaim(demand)
          .flatMap:
            case granted @ Some(_) => ZIO.succeed(granted)
            case None              => claim(demand, asked)

  /**
   * What is left of a caller's patience.
   *
   * @param patience what the caller was granted
   * @param asked    when its call reached this adapter
   * @return the time still to wait, or `None` when the patience is already spent
   */
  private def remainingTime(patience: Duration, asked: Instant): UIO[Option[Duration]] =
    Clock.instant.map: now =>
      val left = patience.minus(Duration.fromInterval(asked, now))
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
