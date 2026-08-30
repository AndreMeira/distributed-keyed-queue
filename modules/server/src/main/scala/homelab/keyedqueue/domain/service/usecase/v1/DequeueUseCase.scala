package homelab.keyedqueue.domain.service.usecase.v1


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ Claimed, Delivery }
import homelab.keyedqueue.domain.request.v1.DequeueRequest
import homelab.keyedqueue.domain.response.v1.DequeueResponse
import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import homelab.keyedqueue.domain.syntax.*
import zio.{ duration2DurationOps, Duration, IO }


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
 * @param validation what makes a request actionable
 * @param maxWait the longest wait this service will honour
 */
final class DequeueUseCase(store: QueueStore, watchdog: Watchdog, validation: QueueInputValidation, maxWait: Duration):

  /**
   * Wait for a message.
   *
   * A caller asking to wait longer than the service allows is clamped rather than refused: patience is a
   * preference, not a mistake, and the response says when the wait actually ended.
   *
   * @param request the queue and how long the caller will wait, clamped to the service's ceiling
   * @return the delivery, or nothing when none became ready in time; aborts with `InvalidRequest` when the
   *         queue is unnamed, or with `QueueError` when the store fails
   */
  def apply(request: DequeueRequest): IO[QueueError, DequeueResponse] =
    val patience = if request.maxWait > maxWait then maxWait else request.maxWait
    validation.parse(request).orFail
      *> watchdog.watch(request.queue)
      *> store.claim(request.queue, patience).map(response)

  /**
   * Present what the store returned as the answer the caller gets.
   *
   * Nothing to report is a *response*, not a failure: a dequeue that waited its full patience and found no
   * work has behaved exactly as asked. Reporting that as an error would push every consumer's quiet case
   * into error handling, and make an idle queue indistinguishable from a broken one.
   *
   * @param claimed what the store handed over, or nothing when the wait elapsed first
   * @return the response, carrying a delivery only when there was one
   */
  private def response(claimed: Option[Claimed]): DequeueResponse = DequeueResponse:
    claimed match {
      case Some(claim) => Some(delivery(claim))
      case None        => None
    }

  /**
   * Present a claim as a delivery: the same message, with the handle that settles it in place of the claim
   * the store hands back.
   *
   * Total, because the store returns a message rather than bytes to be read: whether what it holds is
   * readable is settled before it gets here.
   *
   * @param claimed what the store handed over
   * @return the delivery
   */
  private def delivery(claimed: Claimed): Delivery =
    Delivery(claimed.claim.reference, claimed.message, claimed.attempt, claimed.leaseExpiresAt, claimed.backlogDepth)
