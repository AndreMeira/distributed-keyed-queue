package homelab.keyedqueue.domain.service.usecase


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ Claimed, Delivery }
import homelab.keyedqueue.domain.request.queue.DequeueRequest
import homelab.keyedqueue.domain.response.queue.DequeueResponse
import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.serialisation.EnvelopeCodec
import zio.{ duration2DurationOps, Duration, IO, ZIO }


/**
 * Wait for work on a queue, and hand it to the caller with the lease that comes with it.
 *
 * '''The claim happens in the caller's own fiber.''' A background loop claiming into a buffer would separate
 * the claim from the claimant, which is what creates work claimed for a caller that has since gone — the
 * failure the toolkit's `PollConsumer` spent a design cycle on. Here the fiber that waits is the fiber that
 * receives.
 *
 * @param store where the queue lives
 * @param codec how the stored bytes become an envelope again
 * @param watchdog told about the queue, so its abandoned work is repaired
 * @param maxWait the longest wait this service will honour
 */
final class DequeueUseCase(store: QueueStore, codec: EnvelopeCodec, watchdog: Watchdog, maxWait: Duration):

  /**
   * Wait for a message.
   *
   * @param request the queue and how long the caller will wait, clamped to the service's ceiling
   * @return the delivery, or nothing when none became ready in time; aborts with `Invalid` when the queue is
   *         unnamed, or with `QueueError` when the store fails
   */
  def apply(request: DequeueRequest): IO[QueueError, DequeueResponse] =
    if request.queue.isEmpty then ZIO.fail(QueueError.Invalid("a queue name is required"))
    else
      val patience = if request.maxWait > maxWait then maxWait else request.maxWait
      watchdog.watch(request.queue)
        *> store.claim(request.queue, patience).flatMap(ZIO.foreach(_)(delivery)).map(DequeueResponse.apply)

  /**
   * Present a claim as a delivery: the stored bytes read back, and the handle that settles them.
   *
   * @param claimed what the store handed over
   * @return the delivery; aborts when the stored bytes are not an envelope this version can read
   */
  private def delivery(claimed: Claimed): IO[QueueError, Delivery] =
    codec
      .decode(claimed.payload)
      .map(Delivery(claimed.claim.receipt, _, claimed.attempt, claimed.leaseExpiresAt))
