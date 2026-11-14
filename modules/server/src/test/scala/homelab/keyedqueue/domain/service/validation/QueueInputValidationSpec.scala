package homelab.keyedqueue.domain.service.validation


import homelab.common.Validated
import homelab.common.orFail
import homelab.keyedqueue.SpecHelper.Helper
import homelab.common.error.ValidationError
import homelab.keyedqueue.domain.error.InvalidInput
import homelab.keyedqueue.domain.model.queue.{ Claim, Demand, Message, Renewal, ScheduledSubmission, Settlement, Submission }
import homelab.keyedqueue.domain.model.queue.Settlement.Verdict
import homelab.keyedqueue.domain.request.queue.{ DequeueRequest, EnqueueRequest, ScheduleRequest, SettleRequest }
import homelab.keyedqueue.domain.request.lock.*
import homelab.keyedqueue.domain.request.queue.SettleRequest.MessageOutcome
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
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("QueueInputValidation")(
    test("a settle naming an empty id, or the same id twice, is refused") {
      val empty = SettleRequest(receipt, Chunk(MessageOutcome("", Verdict.Done)), Duration.Zero)
      val twice =
        SettleRequest(
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
      val forged = SettleRequest("not-a-receipt", Chunk(MessageOutcome("m1", Verdict.Done)), Duration.Zero)
      assertTrue(validation.parse(forged).toEither == Left(NonEmptyChunk(InvalidInput.UnreadableReceipt)))
    },
    test("a garbled receipt and an empty batch are reported together") {
      val both = SettleRequest("not-a-receipt", Chunk.empty, Duration.Zero)
      assertTrue(
        validation.parse(both).toEither ==
          Left(NonEmptyChunk(InvalidInput.UnreadableReceipt, InvalidInput.EmptySettle))
      )
    },
    test("a garbled receipt does not hide a bad id") {
      // The reason the parse is one `validate` and not a gate in front of another: staged, the receipt
      // would fail first and the caller would never hear about the id until it had fixed the receipt.
      val both = SettleRequest("not-a-receipt", Chunk(MessageOutcome("", Verdict.Done)), Duration.Zero)
      assertTrue(
        validation.parse(both).toEither ==
          Left(NonEmptyChunk(InvalidInput.UnreadableReceipt, InvalidInput.EmptyDiscardId))
      )
    },
    test("a settle naming nothing is refused") {
      // Refused rather than accepted as a no-op: it would decide nothing while reading, to whoever sent it,
      // like something happened. `Settlement` cannot hold an empty batch, and this is where a caller hears
      // why.
      val none = SettleRequest(receipt, Chunk.empty, Duration.Zero)
      assertTrue(validation.parse(none).toEither == Left(NonEmptyChunk(InvalidInput.EmptySettle)))
    },
    test("a well-formed settle is parsed into what the store takes") {
      // The point of parse rather than validate: what comes back carries MessageIds, a batch known to be
      // non-empty, and a backoff that says "none asked for" rather than zero. None of that is expressible
      // in the request, so nothing downstream can act on the untrusted one by mistake.
      val request =
        SettleRequest(
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
      val request = SettleRequest(receipt, Chunk(MessageOutcome("m1", Verdict.Failed)), 5.seconds)
      assertTrue(validation.parse(request).toEither.map(_.retryAfter) == Right(Some(5.seconds)))
    },
    test("a backoff beyond the ceiling is clamped to it, not refused: the settle's acknowledgements still land") {
      // A backoff holds back the key's whole line, so a mistaken one would otherwise stall it for as long as it
      // asked, up to the ~292 years the codec reads as infinite (#83).
      assertTrue(
        validation.parse(nack(1.hour + 1.milli)).toEither.map(_.retryAfter) == Right(Some(1.hour)),
        validation.parse(nack(Duration.Infinity)).toEither.map(_.retryAfter) == Right(Some(1.hour)),
        validation.parse(nack(1.hour)).toEither.map(_.retryAfter) == Right(Some(1.hour)),
      )
    },
    test("a schedule with a delay within the horizon is accepted as it was asked") {
      val parsed = validation.parse(ScheduleRequest("jobs", Helper.requestMessage("k1"), 5.minutes))
      assertTrue(parsed.toEither.map(_.delay) == Right(5.minutes))
    },
    test("a schedule with no delay, a zero one or a negative one is refused: deliver now is an enqueue") {
      // An absent `deliver_after` reaches the domain as zero, so this is also the refusal for leaving it out.
      val refused = Chunk(Duration.Zero, -1.second).map(schedule).map(_.toEither)
      assertTrue(refused.forall(_ == Left(NonEmptyChunk(InvalidInput.NonPositiveDeliverAfter))))
    },
    test("a schedule beyond the horizon is refused, naming the horizon, rather than clamped") {
      val parsed = schedule(7.days + 1.milli)
      assertTrue(
        parsed.toEither == Left(NonEmptyChunk(InvalidInput.DeliverAfterBeyondHorizon(7.days))),
        schedule(7.days).toEither.isRight,
      )
    },
    test("a schedule's bad delay is reported with its message's and queue's problems") {
      val parsed = validation.parse(ScheduleRequest("", Helper.requestMessage(""), Duration.Zero))
      assertTrue(
        parsed.toEither ==
          Left(NonEmptyChunk(InvalidInput.EmptyQueueName, InvalidInput.EmptyMessageKey, InvalidInput.NonPositiveDeliverAfter))
      )
    },
    test("a message without an id is refused: it is what the store addresses it by") {
      val parsed = validation.parse(EnqueueRequest("jobs", Helper.requestMessage("k1", messageId = "")))
      assertTrue(parsed.toEither == Left(NonEmptyChunk(InvalidInput.EmptyMessageId)))
    },
    test("a message without an encoding is refused: it is how a consumer knows to read the payload") {
      val parsed = validation.parse(EnqueueRequest("jobs", Helper.requestMessage("k1").copy(encoding = "")))
      assertTrue(parsed.toEither == Left(NonEmptyChunk(InvalidInput.EmptyEncoding)))
    },
    test("an empty encoding is reported with the message's other problems") {
      val message = Helper.requestMessage("", messageId = "").copy(encoding = "")
      val parsed  = validation.parse(EnqueueRequest("jobs", message))
      assertTrue(
        parsed.toEither == Left(NonEmptyChunk(InvalidInput.EmptyMessageKey, InvalidInput.EmptyMessageId, InvalidInput.EmptyEncoding))
      )
    },
    test("a well-formed enqueue is parsed into what the store takes") {
      // As with settle: what comes back carries a QueueName, a MessageKey and a MessageId, none of which
      // the request can express — so the use case has nothing unchecked left to reach for.
      val parsed = validation.parse(EnqueueRequest("jobs", Helper.requestMessage("k1")))
      assertTrue(
        parsed.toEither == Right(
          Submission(
            QueueName("jobs"),
            Message(MessageKey("k1"), MessageId("m1"), "test.Text/v1", "application/json", None, Chunk.empty),
          )
        )
      )
    },
    test("a request with nothing wrong passes") {
      val enqueue = validation.parse(EnqueueRequest("jobs", Helper.requestMessage("k1")))
      val dequeue = validation.parse(DequeueRequest("jobs", 1.second, maxBatch = 1))
      assertTrue(enqueue.toEither.isRight, dequeue.toEither.isRight)
    },
    test("two problems in one request are both reported") {
      // The whole reason validation is not a pair of ifs: a caller that got one error, fixed it and got the
      // next would need two round trips to learn what a single answer can tell it.
      for failure <- validation.parse(EnqueueRequest("", Helper.requestMessage(""))).orFail.flip
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
        noQueue <- validation.parse(EnqueueRequest("", Helper.requestMessage("k1"))).orFail.flip
        noKey   <- validation.parse(EnqueueRequest("jobs", Helper.requestMessage(""))).orFail.flip
      yield assertTrue(
        noQueue == ValidationError(NonEmptyChunk(InvalidInput.EmptyQueueName)),
        noKey == ValidationError(NonEmptyChunk(InvalidInput.EmptyMessageKey)),
      )
    },
    test("a queue name holding a colon is refused on every verb that names a queue") {
      // A colon in the name would let one queue and key spell another queue's Redis key (#77).
      val refused = Chunk(
        validation.parse(EnqueueRequest("jobs:msgs:a", Helper.requestMessage("k1"))).toEither,
        validation.parse(ScheduleRequest("jobs:msgs:a", Helper.requestMessage("k1"), 5.minutes)).toEither,
        validation.parse(DequeueRequest("jobs:msgs:a", 1.second, maxBatch = 1)).toEither,
      )
      assertTrue(refused.forall(_.left.map(_.toChunk) == Left(Chunk(InvalidInput.ColonInQueueName))))
    },
    test("a queue name's colon is reported with the request's other problems, and an empty name only as empty") {
      val colon = validation.parse(EnqueueRequest(":", Helper.requestMessage("")))
      val empty = validation.parse(EnqueueRequest("", Helper.requestMessage("k1")))
      assertTrue(
        colon.toEither == Left(NonEmptyChunk(InvalidInput.ColonInQueueName, InvalidInput.EmptyMessageKey)),
        empty.toEither == Left(NonEmptyChunk(InvalidInput.EmptyQueueName)),
      )
    },
    test("a key and a message id may hold colons") {
      val parsed = validation.parse(EnqueueRequest("jobs", Helper.requestMessage("a:ready", messageId = "m:1")))
      assertTrue(parsed.toEither.map(_.message.key) == Right(MessageKey("a:ready")))
    },
    test("a settle whose receipt names a queue holding a colon is refused as unreadable") {
      val forged = Claim(QueueName("jobs:msgs:a"), MessageKey("k1"), Token(1)).reference
      val parsed = validation.parse(SettleRequest(forged, Chunk(MessageOutcome("m1", Verdict.Done)), Duration.Zero))
      assertTrue(parsed.toEither == Left(NonEmptyChunk(InvalidInput.UnreadableReceipt)))
    },
    test("a dequeue that will not wait is refused, and so is one that never said") {
      // Rejected rather than served: a claim that will not wait is a different operation from the one this
      // API offers. An absent `max_wait` is `None` in the `v1.DequeueRequest`, which `Inbound` reads as zero, so
      // it lands here too — a caller has to say how long it is prepared to wait.
      for
        impatient <- validation.parse(DequeueRequest("jobs", Duration.Zero, maxBatch = 1)).orFail.flip
        backwards <- validation.parse(DequeueRequest("jobs", (-1).second, maxBatch = 1)).orFail.flip
        // Accumulated with the others rather than short-circuiting, which is the point of the whole scheme.
        both      <- validation.parse(DequeueRequest("", Duration.Zero, maxBatch = -1)).orFail.flip
      yield assertTrue(
        impatient == ValidationError(NonEmptyChunk(InvalidInput.NonPositiveMaxWait)),
        backwards == ValidationError(NonEmptyChunk(InvalidInput.NonPositiveMaxWait)),
        both == ValidationError(
          NonEmptyChunk(InvalidInput.EmptyQueueName, InvalidInput.NonPositiveMaxWait, InvalidInput.NegativeMaxBatch)
        ),
      )
    },
    test("a dequeue asking for more than the service offers is clamped, not refused") {
      // What the parse buys: a Demand is bounded by construction, so nothing downstream can be handed an
      // hour-long wait or a batch of a thousand, and nothing downstream has to remember to check.
      val greedy = DequeueRequest("jobs", 1.hour, maxBatch = 1000)
      assertTrue(
        validation.parse(greedy).toEither == Right(Demand(QueueName("jobs"), 30.seconds, 32))
      )
    },
    test("a dequeue asking for nothing in particular gets one message") {
      // Zero and one mean the same thing: a caller that says nothing about batching is not asking for an
      // empty batch, it is not asking about batching.
      val quiet = DequeueRequest("jobs", 1.second, maxBatch = 0)
      assertTrue(validation.parse(quiet).toEither.map(_.batch) == Right(1))
    },
    test("a dequeue only needs a queue, however long the caller wants to wait") {
      // Patience is a preference, not a mistake.
      val patient = validation.parse(DequeueRequest("jobs", 1.hour, maxBatch = 1))
      for unnamed <- validation.parse(DequeueRequest("", 1.second, maxBatch = 1)).orFail.flip
      yield assertTrue(
        patient.toEither.isRight,
        unnamed == ValidationError(NonEmptyChunk(InvalidInput.EmptyQueueName)),
      )
    },
  )

  private object Support:

    val validation = QueueInputValidation(
      QueueInputValidation.Config(maxWait = 30.seconds, maxBatchLimit = 32, maxScheduleAhead = 7.days, maxRetryAfter = 1.hour)
    )

    /**
     * A well-formed schedule with the delay given, parsed.
     *
     * @param delay the delay to ask for
     * @return what the parse made of it
     */
    def schedule(delay: Duration): Validated[ScheduledSubmission] =
      validation.parse(ScheduleRequest("jobs", Helper.requestMessage("k1"), delay))

    /**
     * A settle nacking one message of [[claim]], asking its key to wait.
     *
     * @param wait the backoff asked for
     * @return the request
     */
    def nack(wait: Duration): SettleRequest =
      SettleRequest(receipt, Chunk(MessageOutcome("m1", Verdict.Failed)), wait)

    /** The claim a settle names, and the receipt a consumer would have been handed for it. */
    val claim   = Claim(QueueName("jobs"), MessageKey("k1"), Token(1))
    val receipt = claim.reference
