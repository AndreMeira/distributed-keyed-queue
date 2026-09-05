package homelab.keyedqueue.domain.service.validation


import homelab.common.error.ValidationError
import homelab.keyedqueue.domain.error.InvalidInput
import homelab.keyedqueue.domain.model.{ Claim, Demand, Message, Renewal, Settlement, Submission }
import homelab.keyedqueue.domain.request.v1.QueueRequest
import homelab.keyedqueue.domain.request.v1.QueueRequest.MessageOutcome
import homelab.keyedqueue.domain.syntax.*
import homelab.keyedqueue.domain.types.*
import zio.*
import zio.test.*


/**
 * That validation accumulates, and that collapsing it keeps everything it found.
 *
 * The property worth a test is the one an `if` would silently lose: a request with two problems must come
 * back naming two problems. Everything else here is a guard on the wording a caller sees.
 *
 * Most of these need no effect at all: a validation is a value, so what it found can be read off it
 * directly. Only the tests about `orFail` — the step that turns problems into a refusal — are effectful.
 */
object QueueInputValidationSpec extends ZIOSpecDefault:

  private val validation = QueueInputValidation(QueueInputValidation.Config(maxWait = 30.seconds, maxBatchLimit = 32))

  /** The claim a settle names, and the receipt a consumer would have been handed for it. */
  private val claim   = Claim(QueueName("jobs"), MessageKey("k1"), Token(1))
  private val receipt = claim.reference

  /** A message as it arrives, with whatever key the test is about; nothing else here is under test. */
  private def message(key: String, messageId: String = "m1"): QueueRequest.Enqueue.Message =
    QueueRequest.Enqueue.Message(key, messageId, payloadType = "test.Text/v1", Encoding.Json, None, Chunk.empty)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("QueueInputValidation")(
    test("a settle naming an empty id, or the same id twice, is refused") {
      val empty = QueueRequest.Settle(receipt, Chunk(MessageOutcome("", Verdict.Done)), Duration.Zero)
      val twice =
        QueueRequest.Settle(
          receipt,
          Chunk(MessageOutcome("m1", Verdict.Done), MessageOutcome("m1", Verdict.Done)),
          Duration.Zero,
        )
      assertTrue(
        validation.parse(empty).toEither == Left(NonEmptyChunk(InvalidInput.EmptyDiscardId)),
        validation.parse(twice).toEither == Left(NonEmptyChunk(InvalidInput.DuplicateDiscardId)),
      )
    },
    test("a receipt this service never issued is refused, not reported stale") {
      // The distinction the use case used to blur: a string that was never a receipt is a caller's bug,
      // while a receipt whose claim has been revoked is a race it lost. Only the second is `Stale`, and
      // only the store can answer it.
      val forged = QueueRequest.Settle("not-a-receipt", Chunk(MessageOutcome("m1", Verdict.Done)), Duration.Zero)
      assertTrue(validation.parse(forged).toEither == Left(NonEmptyChunk(InvalidInput.UnreadableReceipt)))
    },
    test("a garbled receipt and an empty batch are reported together") {
      val both = QueueRequest.Settle("not-a-receipt", Chunk.empty, Duration.Zero)
      assertTrue(
        validation.parse(both).toEither ==
          Left(NonEmptyChunk(InvalidInput.UnreadableReceipt, InvalidInput.EmptySettle))
      )
    },
    test("a garbled receipt does not hide a bad id") {
      // The reason the parse is one `validate` and not a gate in front of another: staged, the receipt
      // would fail first and the caller would never hear about the id until it had fixed the receipt.
      val both = QueueRequest.Settle("not-a-receipt", Chunk(MessageOutcome("", Verdict.Done)), Duration.Zero)
      assertTrue(
        validation.parse(both).toEither ==
          Left(NonEmptyChunk(InvalidInput.UnreadableReceipt, InvalidInput.EmptyDiscardId))
      )
    },
    test("a settle naming nothing is refused") {
      // Refused rather than accepted as a no-op: it would decide nothing while reading, to whoever sent it,
      // like something happened. `Settlement` cannot hold an empty batch, and this is where a caller hears
      // why.
      val none = QueueRequest.Settle(receipt, Chunk.empty, Duration.Zero)
      assertTrue(validation.parse(none).toEither == Left(NonEmptyChunk(InvalidInput.EmptySettle)))
    },
    test("a well-formed settle is parsed into what the store takes") {
      // The point of parse rather than validate: what comes back carries MessageIds, a batch known to be
      // non-empty, and a backoff that says "none asked for" rather than zero. None of that is expressible
      // in the request, so nothing downstream can act on the untrusted one by mistake.
      val request =
        QueueRequest.Settle(
          receipt,
          Chunk(MessageOutcome("m1", Verdict.Done), MessageOutcome("m2", Verdict.Failed)),
          Duration.Zero,
        )
      val parsed  = validation.parse(request).toEither
      assertTrue(
        parsed == Right(
          Settlement(
            claim,
            NonEmptyChunk(
              Settlement.Outcome(MessageId("m1"), Verdict.Done),
              Settlement.Outcome(MessageId("m2"), Verdict.Failed),
            ),
            None,
          )
        )
      )
    },
    test("a backoff a caller did ask for survives the parse") {
      val request = QueueRequest.Settle(receipt, Chunk(MessageOutcome("m1", Verdict.Failed)), 5.seconds)
      assertTrue(validation.parse(request).toEither.map(_.retryAfter) == Right(Some(5.seconds)))
    },
    test("a message without an id is refused: it is what the store addresses it by") {
      val parsed = validation.parse(QueueRequest.Enqueue("jobs", message("k1", messageId = "")))
      assertTrue(parsed.toEither == Left(NonEmptyChunk(InvalidInput.EmptyMessageId)))
    },
    test("a well-formed enqueue is parsed into what the store takes") {
      // As with settle: what comes back carries a QueueName, a MessageKey and a MessageId, none of which
      // the request can express — so the use case has nothing unchecked left to reach for.
      val parsed = validation.parse(QueueRequest.Enqueue("jobs", message("k1")))
      assertTrue(
        parsed.toEither == Right(
          Submission(
            QueueName("jobs"),
            Message(MessageKey("k1"), MessageId("m1"), "test.Text/v1", Encoding.Json, None, Chunk.empty),
          )
        )
      )
    },
    test("a request with nothing wrong passes") {
      val enqueue = validation.parse(QueueRequest.Enqueue("jobs", message("k1")))
      val dequeue = validation.parse(QueueRequest.Dequeue("jobs", 1.second, maxBatch = 1))
      assertTrue(enqueue.toEither.isRight, dequeue.toEither.isRight)
    },
    test("two problems in one request are both reported") {
      // The whole reason validation is not a pair of ifs: a caller that got one error, fixed it and got the
      // next would need two round trips to learn what a single answer can tell it.
      for failure <- validation.parse(QueueRequest.Enqueue("", message(""))).orFail.flip
      yield assertTrue(
        failure == ValidationError(NonEmptyChunk(InvalidInput.EmptyQueueName, InvalidInput.EmptyMessageKey)),
        // Both problems reach the caller. Asserted by containment rather than as one joined string: how
        // the toolkit renders an aggregate is its business, and pinning it here would make a rendering
        // change in `ValidationError` a failure in this service.
        failure.message.contains("a queue name is required"),
        failure.message.contains("a message key is required"),
      )
    },
    test("one problem is reported alone") {
      for
        noQueue <- validation.parse(QueueRequest.Enqueue("", message("k1"))).orFail.flip
        noKey   <- validation.parse(QueueRequest.Enqueue("jobs", message(""))).orFail.flip
      yield assertTrue(
        noQueue == ValidationError(NonEmptyChunk(InvalidInput.EmptyQueueName)),
        noKey == ValidationError(NonEmptyChunk(InvalidInput.EmptyMessageKey)),
      )
    },
    test("a dequeue asking for more than the service offers is clamped, not refused") {
      // What the parse buys: a Demand is bounded by construction, so nothing downstream can be handed an
      // hour-long wait or a batch of a thousand, and nothing downstream has to remember to check.
      val greedy = QueueRequest.Dequeue("jobs", 1.hour, maxBatch = 1000)
      assertTrue(
        validation.parse(greedy).toEither == Right(Demand(QueueName("jobs"), 30.seconds, 32))
      )
    },
    test("a dequeue asking for nothing in particular gets one message") {
      // Zero and one mean the same thing: a caller that says nothing about batching is not asking for an
      // empty batch, it is not asking about batching.
      val quiet = QueueRequest.Dequeue("jobs", 1.second, maxBatch = 0)
      assertTrue(validation.parse(quiet).toEither.map(_.batch) == Right(1))
    },
    test("a dequeue only needs a queue, however long the caller wants to wait") {
      // Patience is a preference, not a mistake.
      val patient = validation.parse(QueueRequest.Dequeue("jobs", 1.hour, maxBatch = 1))
      for unnamed <- validation.parse(QueueRequest.Dequeue("", 1.second, maxBatch = 1)).orFail.flip
      yield assertTrue(
        patient.toEither.isRight,
        unnamed == ValidationError(NonEmptyChunk(InvalidInput.EmptyQueueName)),
      )
    },
  )
