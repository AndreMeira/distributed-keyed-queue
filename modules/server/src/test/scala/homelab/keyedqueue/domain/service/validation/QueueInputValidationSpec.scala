package homelab.keyedqueue.domain.service.validation


import homelab.keyedqueue.domain.error.{ InvalidInput, QueueError }
import homelab.keyedqueue.domain.model.Message
import homelab.keyedqueue.domain.request.v1.{ DequeueRequest, EnqueueRequest, MessageOutcome, SettleRequest }
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
    Message(MessageKey(key), messageId = MessageId("m1"), payloadType = "test.Text/v1", Encoding.Json, None, Chunk.empty)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("QueueInputValidation")(
    test("a settle naming an empty id, or the same id twice, is refused") {
      val empty = SettleRequest(ClaimRef("anything"), Chunk(MessageOutcome(MessageId(""), Verdict.Done)), Duration.Zero)
      val twice =
        SettleRequest(
          ClaimRef("anything"),
          Chunk(MessageOutcome(MessageId("m1"), Verdict.Done), MessageOutcome(MessageId("m1"), Verdict.Done)),
          Duration.Zero,
        )
      for
        first  <- validation.parse(empty).flip
        second <- validation.parse(twice).flip
      yield assertTrue(first == InvalidInput.EmptyDiscardId, second == InvalidInput.DuplicateDiscardId)
    },
    test("a settle naming nothing, or some distinct ids, passes") {
      val none = SettleRequest(ClaimRef("anything"), Chunk.empty, Duration.Zero)
      val some =
        SettleRequest(
          ClaimRef("anything"),
          Chunk(MessageOutcome(MessageId("m1"), Verdict.Done), MessageOutcome(MessageId("m2"), Verdict.Done)),
          Duration.Zero,
        )
      for
        first  <- validation.parse(none).either
        second <- validation.parse(some).either
      yield assertTrue(first.isRight, second.isRight)
    },
    test("a message without an id is refused: it is what the store addresses it by") {
      val nameless = Message(MessageKey("k1"), MessageId(""), "test.Text/v1", Encoding.Json, None, Chunk.empty)
      for problems <- validation.parse(EnqueueRequest(QueueName("jobs"), nameless)).flip
      yield assertTrue(problems == InvalidInput.EmptyMessageId)
    },
    test("a request with nothing wrong passes") {
      for
        enqueue <- validation.parse(EnqueueRequest(QueueName("jobs"), message("k1"))).exit
        dequeue <- validation.parse(DequeueRequest(QueueName("jobs"), 1.second, maxBatch = 1)).exit
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
        patient <- validation.parse(DequeueRequest(QueueName("jobs"), 1.hour, maxBatch = 1)).exit
        unnamed <- validation.parse(DequeueRequest(QueueName(""), 1.second, maxBatch = 1)).orFail.flip
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
