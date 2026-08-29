package homelab.keyedqueue.domain.service.validation


import homelab.keyedqueue.domain.error.{ InvalidInput, QueueError }
import homelab.keyedqueue.domain.model.Message
import homelab.keyedqueue.domain.request.v1.{ DequeueRequest, EnqueueRequest }
import homelab.keyedqueue.domain.syntax.*
import homelab.keyedqueue.domain.types.*
import zio.*
import zio.test.*


/**
 * That validation accumulates, and that collapsing it keeps everything it found.
 *
 * The property worth a test is the one an `if` would silently lose: a request with two problems must come
 * back naming two problems. Everything else here is a guard on the wording a caller sees.
 */
object QueueInputValidationSpec extends ZIOSpecDefault:

  private val validation = QueueInputValidation()

  /** A message with whatever key the test is about; nothing else here is under test. */
  private def message(key: String): Message =
    Message(MessageKey(key), messageId = "m1", payloadType = "test.Text/v1", Encoding.Json, None, Chunk.empty)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("QueueInputValidation")(
    test("a request with nothing wrong passes") {
      for
        enqueue <- validation.parse(EnqueueRequest(QueueName("jobs"), message("k1"))).exit
        dequeue <- validation.parse(DequeueRequest(QueueName("jobs"), 1.second)).exit
      yield assertTrue(enqueue.isSuccess, dequeue.isSuccess)
    },
    test("two problems in one request are both reported") {
      // The whole reason validation is not a pair of ifs: a caller that got one error, fixed it and got the
      // next would need two round trips to learn what a single answer can tell it.
      for failure <- validation.parse(EnqueueRequest(QueueName(""), message(""))).orFail.flip
      yield assertTrue(
        failure == QueueError.InvalidRequest(NonEmptyChunk(InvalidInput.EmptyQueueName, InvalidInput.EmptyMessageKey)),
        failure.message == "a queue name is required; a message key is required: it is what ordering is defined by",
      )
    },
    test("one problem is reported alone") {
      for
        noQueue <- validation.parse(EnqueueRequest(QueueName(""), message("k1"))).orFail.flip
        noKey   <- validation.parse(EnqueueRequest(QueueName("jobs"), message(""))).orFail.flip
      yield assertTrue(
        noQueue == QueueError.InvalidRequest(NonEmptyChunk(InvalidInput.EmptyQueueName)),
        noKey == QueueError.InvalidRequest(NonEmptyChunk(InvalidInput.EmptyMessageKey)),
      )
    },
    test("a dequeue only needs a queue, however long the caller wants to wait") {
      // Patience is clamped by the apply case, not refused here — an hour is a preference, not a mistake.
      for
        patient <- validation.parse(DequeueRequest(QueueName("jobs"), 1.hour)).exit
        unnamed <- validation.parse(DequeueRequest(QueueName(""), 1.second)).orFail.flip
      yield assertTrue(
        patient.isSuccess,
        unnamed == QueueError.InvalidRequest(NonEmptyChunk(InvalidInput.EmptyQueueName)),
      )
    },
    test("a defect is not reported as the caller's mistake") {
      // orFail reads typed failures out of the cause; a cause carrying none of them is ours, and must
      // stay a defect rather than being handed to a caller as INVALID_ARGUMENT.
      for died <- ZIO.dieMessage("boom").orFail.exit
      yield assertTrue(died.isFailure, !died.isSuccess, died.causeOption.exists(_.isDie))
    },
  )
