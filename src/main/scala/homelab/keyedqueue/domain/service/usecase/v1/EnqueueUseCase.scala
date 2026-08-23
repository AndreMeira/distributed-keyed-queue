package homelab.keyedqueue.domain.service.usecase.v1


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.request.v1.EnqueueRequest
import homelab.keyedqueue.domain.response.v1.EnqueueResponse
import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.serialisation.EnvelopeCodec
import zio.{ IO, ZIO }


/**
 * Accept a message for a key.
 *
 * @param store where the queue lives
 * @param codec how an envelope becomes the bytes the store holds
 * @param watchdog told about the queue, so its abandoned work is repaired
 */
final class EnqueueUseCase(store: QueueStore, codec: EnvelopeCodec, watchdog: Watchdog):

  /**
   * Validate and append.
   *
   * The key is rejected when empty rather than treated as "no ordering": every message sharing one empty
   * key would be serialised behind the others, the opposite of what leaving it blank suggests.
   *
   * @param request the queue and the message
   * @return the key's depth after the append; aborts with `Invalid` when the request cannot be acted on, or
   *         with `QueueError` when the store fails
   */
  def apply(request: EnqueueRequest): IO[QueueError, EnqueueResponse] =
    if request.queue.isEmpty then ZIO.fail(QueueError.Invalid("a queue name is required"))
    else if request.envelope.key.isEmpty then ZIO.fail(QueueError.Invalid("a message key is required: it is what ordering is defined by"))
    else
      watchdog.watch(request.queue)
        *> store
          .enqueue(request.queue, request.envelope.key, codec.encode(request.envelope))
          .map(EnqueueResponse.apply)
