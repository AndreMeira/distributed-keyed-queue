package homelab.keyedqueue.domain.service.usecase


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.Claimed
import homelab.keyedqueue.domain.types.QueueName
import zio.{ duration2DurationOps, Duration, IO, ZIO }


/**
 * Wait for work on a queue, and hand it to the caller with the lease that comes with it.
 *
 * '''The claim happens in the caller's own fiber.''' A background loop claiming into a buffer would separate
 * the claim from the claimant, which is what creates work claimed for a caller that has since gone — the
 * failure the toolkit's `PollConsumer` spent a design cycle on. Here the fiber that waits is the fiber that
 * receives.
 *
 * @param claim what actually waits: a borrowed connection, since only those may block
 * @param maxWait the longest wait this service will honour
 */
final class DequeueUseCase(claim: (QueueName, Duration) => IO[QueueError, Option[Claimed]], maxWait: Duration):

  /**
   * Wait up to `wait` for a message.
   *
   * @param queue the queue to take from
   * @param wait how long the caller is prepared to wait; clamped to the service's ceiling
   * @return the claim, or `None` when nothing became ready in time; aborts with `Invalid` when the queue is
   *         unnamed, or with `QueueError` when the store fails
   */
  def apply(queue: QueueName, wait: Duration): IO[QueueError, Option[Claimed]] =
    if queue.isEmpty then ZIO.fail(QueueError.Invalid("a queue name is required"))
    else claim(queue, if wait > maxWait then maxWait else wait)
