package homelab.keyedqueue.domain.service.usecase.v1


import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.keyedqueue.domain.model.Claimed
import homelab.keyedqueue.domain.request.v1.QueueRequest
import homelab.keyedqueue.domain.response.v1.QueueResponse
import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import homelab.keyedqueue.domain.syntax.*
import zio.{ Chunk, Duration, IO, NonEmptyChunk, duration2DurationOps }


/**
 * Wait for work on a queue, and hand it to the caller with the lease that comes with it.
 *
 * '''The claim happens in the caller's own fiber.''' A background loop claiming into a buffer would separate
 * the claim from the claimant, which is what creates work claimed for a caller that has since gone — the
 * failure the toolkit's `PollConsumer` spent a design cycle on. Here the fiber that waits is the fiber that
 * receives.
 *
 * @param store where the queue lives
 * @param watchdog told about the queue, so its abandoned work is repaired
 * @param validation what turns a request into a demand this service will honour
 */
final class DequeueUseCase(store: QueueStore, watchdog: Watchdog, validation: QueueInputValidation):

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
  def apply(request: QueueRequest.Dequeue): IO[ApplicationError, QueueResponse.Dequeue] =
    validation.parse(request).orFail.flatMap { demand =>
      watchdog.watch(demand.queue) *> store.claim(demand).map(response)
    }

  /**
   * Present what the store returned as the answer the caller gets.
   *
   * Nothing to report is a *response*, not a failure: a dequeue that waited its full patience and found no
   * work has behaved exactly as asked. Reporting that as an error would push every consumer's quiet case
   * into error handling, and make an idle queue indistinguishable from a broken one.
   *
   * @param claimed what the store handed over, or nothing when the wait elapsed first
   * @return the response, carrying a claim only when there was one
   */
  private def response(claimed: Option[Claimed]): QueueResponse.Dequeue =
    claimed match {
      case None          => QueueResponse.Dequeue.Empty
      case Some(claimed) => QueueResponse.Dequeue.fromClaimed(claimed)
    }
