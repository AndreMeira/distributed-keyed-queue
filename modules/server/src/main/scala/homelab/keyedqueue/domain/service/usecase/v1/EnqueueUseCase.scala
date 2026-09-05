package homelab.keyedqueue.domain.service.usecase.v1


import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.keyedqueue.domain.request.v1.QueueRequest
import homelab.keyedqueue.domain.response.v1.QueueResponse
import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import homelab.keyedqueue.domain.syntax.*
import zio.IO


/**
 * Accept a message for a key.
 *
 * @param store where the queue lives
 * @param watchdog told about the queue, so its abandoned work is repaired
 * @param validation what makes a request actionable
 */
final class EnqueueUseCase(store: QueueStore, watchdog: Watchdog, validation: QueueInputValidation):

  /**
   * Parse, then append.
   *
   * `orFail` is where accumulation stops: the parse reports every problem it found, and this is the point
   * at which the use case decides it will not proceed with any of them. What comes back is a `Submission`
   * — the only thing the store accepts — so nothing here can reach past the parse into what the caller
   * actually sent.
   *
   * @param request the queue and the message, untrusted
   * @return the key's depth after the append; aborts with `ValidationError` naming everything wrong with the
   *         request, or with `ApplicationError` when the store fails
   */
  def apply(request: QueueRequest.Enqueue): IO[ApplicationError, QueueResponse.Enqueue] =
    validation.parse(request).orFail.flatMap { submission =>
      watchdog.watch(submission.queue)
        *> store.enqueue(submission).map(QueueResponse.Enqueue.apply)
    }
