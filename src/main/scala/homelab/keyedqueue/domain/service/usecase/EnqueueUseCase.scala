package homelab.keyedqueue.domain.service.usecase


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import zio.{ Chunk, IO }


/**
 * Accept a message for a key.
 *
 * @param store where the queue lives
 */
final class EnqueueUseCase(store: QueueStore):

  /**
   * Validate and append.
   *
   * The key is rejected when empty rather than treated as "no ordering": every message sharing one empty key
   * would be serialised behind the others, which is the opposite of what a caller leaving it blank expects.
   *
   * @param queue the queue to append to
   * @param key the key that orders this message
   * @param payload the encoded message
   * @return the key's queue depth after the append; aborts with `Invalid` when the key is empty, or with
   *         `QueueError` when the store fails
   */
  def apply(queue: QueueName, key: MessageKey, payload: Chunk[Byte]): IO[QueueError, Long] =
    if queue.isEmpty then fail("a queue name is required")
    else if key.isEmpty then fail("a message key is required: it is what ordering is defined by")
    else store.enqueue(queue, key, payload)

  private def fail(reason: String): IO[QueueError, Nothing] = zio.ZIO.fail(QueueError.Invalid(reason))
