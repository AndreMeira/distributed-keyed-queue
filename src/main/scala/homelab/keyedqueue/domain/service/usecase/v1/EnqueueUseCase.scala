package homelab.keyedqueue.domain.service.usecase.v1


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.request.v1.EnqueueRequest
import homelab.keyedqueue.domain.response.v1.EnqueueResponse
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
   * Validate, then append.
   *
   * `orFail` is where accumulation stops: the validator reports every problem it found, and this is the
   * point at which the use case decides it will not proceed with any of them.
   *
   * @param request the queue and the message
   * @return the key's depth after the append; aborts with `InvalidRequest` naming everything wrong with the
   *         request, or with `QueueError` when the store fails
   */
  def apply(request: EnqueueRequest): IO[QueueError, EnqueueResponse] =
    validation.parse(request).orFail
      *> watchdog.watch(request.queue)
      *> store.enqueue(request.queue, request.message).map(EnqueueResponse.apply)
