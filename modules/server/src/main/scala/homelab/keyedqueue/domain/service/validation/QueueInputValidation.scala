package homelab.keyedqueue.domain.service.validation


import homelab.keyedqueue.domain.error.InvalidInput
import homelab.keyedqueue.domain.model.{ Claim, Demand, Message, Renewal, Settlement, Submission }
import homelab.keyedqueue.domain.request.v1.QueueRequest
import homelab.keyedqueue.domain.syntax.Validated
import homelab.keyedqueue.domain.types.*
import zio.prelude.Validation
import zio.duration2DurationOps
import zio.{ Chunk, Duration, NonEmptyChunk }


/**
 * The crossing from what a caller sent to what this service will act on: every problem with a request, in
 * one pass.
 *
 * '''Parses, it does not check.''' Each method returns the type the store accepts — `Submission`, `Demand`,
 * `Settlement` — rather than a verdict on the request it was given. A use case therefore cannot proceed
 * with the unchecked thing: the only way to hold something the store takes is to have come through here.
 * The domain types those carry — a `QueueName`, a `MessageId`, a `Claim` — are the evidence that someone
 * looked.
 *
 * '''A class, though it holds only limits.''' It is wired as a dependency (see [[Module]]) rather than
 * called as an object, so a use case declares that it parses, a test can substitute one that refuses
 * everything, and the day a check needs the store — is this queue known? is this key parked? — that check
 * arrives as a constructor parameter instead of a rewrite of every call site. What it holds today is
 * [[QueueInputValidation.Config]], the bounds a `Demand` may not exceed.
 *
 * '''A receipt is read here; whether it is still live is not.''' A settle names a claim, and a string that
 * was never a receipt is refused as `UnreadableReceipt` — nothing issued it, and no retry makes it valid.
 * Whether the claim it names is still held is a different question, answered by the store's fence and
 * reported as `Stale` rather than refused.
 *
 * `Heartbeat` is parsed by `HeartbeatUseCase` instead. It carries nothing but receipts, and an unreadable
 * one is *not* an error there — it comes back among what the consumer has lost — so that parse cannot
 * fail. A total method among these would have to promise a failure it could never deliver.
 */
final class QueueInputValidation(config: QueueInputValidation.Config):

  /**
   * Everything `Enqueue` needs to be actionable.
   *
   * The checks are combined with `Validation.validate` rather than sequenced, so a request that names no
   * queue *and* no key comes back naming both. That is the only reason this is not a row of `if`s.
   *
   * The id is required because the store keys messages by it: two queued under one id for one key are one
   * message, and an empty id would collapse every unnamed message on that key into a single entry.
   *
   * @param request what the caller sent, untrusted
   * @return the submission to hand the store; accumulates `EmptyQueueName`, `EmptyMessageKey` and
   *         `EmptyMessageId`
   */
  def parse(request: QueueRequest.Enqueue): Validated[Submission] =
    Validation
      .validate(queue(request.queue), message(request.message))
      .map(Submission.apply)

  /**
   * Everything `Dequeue` needs to be actionable.
   *
   * The queue, and that the batch size is not negative. How *much* a caller asks for — of patience or of
   * messages — is clamped rather than refused, because asking for more than the service offers is not a
   * mistake. Asking for a negative amount is.
   *
   * '''Clamping is part of the parse, not of what follows it.''' `Demand` means "within this service's
   * limits", and a value that means that has to be built somewhere that knows them — which is why this
   * class carries [[QueueInputValidation.Config]]. Clamped downstream instead, the bounded and the
   * unbounded value would have the same type, and nothing would say which one a caller of the store held.
   *
   * @param request what the caller sent, untrusted
   * @return the demand to hand the store, bounded; accumulates `EmptyQueueName` and `NegativeMaxBatch`
   */
  def parse(request: QueueRequest.Dequeue): Validated[Demand] =
    Validation
      .validate(
        queue(request.queue),
        CommonValidation.nonNegative(request.maxBatch, InvalidInput.NegativeMaxBatch),
      )
      .map((name, batch) => Demand(name, patience(request.maxWait), this.batch(batch)))

  /**
   * Everything `Settle` needs to be actionable.
   *
   * Only the ids named. The receipt is not checked here — see the note on this class — and each verdict is
   * an enum the wire layer has already refused if unspecified.
   *
   * Whether those ids are actually owned by the claim is deliberately *not* checked: the script looks them
   * up under the claim, where the answer cannot go stale between the check and the act, and an id it does
   * not own is simply ignored.
   *
   * @param request what the caller sent, untrusted
   * @return the settlement to hand the store; accumulates `UnreadableReceipt`, `EmptySettle`,
   *         `EmptyDiscardId` and `DuplicateDiscardId`
   */
  def parse(request: QueueRequest.Settle): Validated[Settlement] =
    Validation
      .validate(
        receipt(request.receipt),
        batch(request.outcomes),
        distinct(request.outcomes.map(_.messageId)),
      )
      .map((claim, settled, _) => Settlement(claim, settled, backoff(request.retryAfter)))

  /**
   * A receipt must be one this service issued.
   *
   * The receipt is the only field whose parse produces something the caller could not have written: a
   * `Claim` names a queue, a key and a token, and getting one back is what says the string was ours.
   * Whether that claim is still *live* is a different question, answered by the store's fence — being
   * unreadable and being revoked are not the same failure and are not reported the same way.
   *
   * @param value the receipt as it arrived
   * @return the claim it names; fails with `UnreadableReceipt` when it names none
   */
  private def receipt(value: String): Validated[Claim] =
    Claim.fromReference(value) match
      case Some(claim) => Validation.succeed(claim)
      case None        => Validation.fail(InvalidInput.UnreadableReceipt)

  /**
   * The messages a settle names, parsed, and known to be at least one.
   *
   * Both halves belong together: `Settlement` needs a `NonEmptyChunk`, and this is the only place that can
   * hand one over. Checking emptiness *before* parsing the elements — rather than as a step in front of it
   * — is what keeps the whole parse to a single `validate`, so a bad receipt and a bad id are reported in
   * the same answer instead of one hiding the other.
   *
   * Nothing accumulates against `EmptySettle`, and nothing needs to: a settle that names no messages has
   * no ids to be wrong about.
   *
   * @param outcomes what the caller sent
   * @return them in domain terms, non-empty; fails with `EmptySettle` when there are none, or accumulates
   *         `EmptyDiscardId` for each that names nothing
   */
  private def batch(outcomes: Chunk[QueueRequest.MessageOutcome]): Validated[NonEmptyChunk[Settlement.Outcome]] =
    NonEmptyChunk.fromChunk(outcomes) match
      case Some(named) => Validation.validateAll(named.map(outcome))
      case None        => Validation.fail(InvalidInput.EmptySettle)

  /**
   * One named message becomes one the store can address.
   *
   * @param outcome what the caller said about it
   * @return the outcome in domain terms; fails with `EmptyDiscardId` when it names nothing
   */
  private def outcome(outcome: QueueRequest.MessageOutcome): Validated[Settlement.Outcome] =
    CommonValidation
      .nonEmpty(outcome.messageId, InvalidInput.EmptyDiscardId)
      .map(id => Settlement.Outcome(MessageId(id), outcome.outcome))

  /**
   * A zero backoff is no backoff.
   *
   * An absent `retry_after` decodes to zero, and "wait no time" and "did not ask to wait" are the same
   * request — so the domain says it once, as `None`, rather than carrying a sentinel the store has to know
   * about.
   *
   * @param retryAfter what the caller asked for
   * @return the wait, or `None` when none was asked for
   */
  private def backoff(retryAfter: Duration): Option[Duration] =
    Option.when(retryAfter.toMillis > 0)(retryAfter)

  /**
   * How long this service will actually wait.
   *
   * A caller asking for longer is not refused: patience is a preference, and the response says when the
   * wait ended. What it may not do is park a connection for as long as it likes.
   *
   * @param asked what the caller is prepared to wait
   * @return that, or the service's ceiling, whichever is shorter
   */
  private def patience(asked: Duration): Duration =
    if asked > config.maxWait then config.maxWait else asked

  /**
   * How many messages this claim may take.
   *
   * Zero and one mean the same thing — one message — so the floor is one rather than a refusal: a caller
   * that says nothing about batching gets the unbatched behaviour.
   *
   * @param asked how many the caller wants, already known not to be negative
   * @return that, bounded to one at the bottom and the service's limit at the top
   */
  private def batch(asked: Int): Int =
    asked.max(1).min(config.maxBatchLimit)

  /**
   * A queue must be named, and naming it is what makes it a [[QueueName]].
   *
   * @param value the name as it arrived
   * @return the name; fails with `EmptyQueueName`
   */
  private def queue(value: String): Validated[QueueName] =
    CommonValidation.nonEmpty(value, InvalidInput.EmptyQueueName).map(QueueName.apply)

  /**
   * A message the store can file: keyed, and addressable by its own name.
   *
   * The two checks accumulate, so a message with neither comes back saying both. Everything else crosses
   * unchanged — the payload is cargo, and `encoding` was made total at the wire.
   *
   * `payloadType` is deliberately not checked here, matching what this service did before the parse
   * existed. An empty one is storable and meaningless, which is worth refusing one day; doing it now would
   * hide a contract change inside a refactor.
   *
   * @param message the message as it arrived
   * @return it in domain terms; accumulates `EmptyMessageKey` and `EmptyMessageId`
   */
  private def message(message: QueueRequest.Enqueue.Message): Validated[Message] =
    Validation
      .validate(
        CommonValidation.nonEmpty(message.key, InvalidInput.EmptyMessageKey).map(MessageKey.apply),
        CommonValidation.nonEmpty(message.messageId, InvalidInput.EmptyMessageId).map(MessageId.apply),
      )
      .map((key, id) => Message(key, id, message.payloadType, message.encoding, message.sentAt, message.payload))

  /**
   * The same id must not be named twice in one settle.
   *
   * Harmless to the store — settling an id twice finds it already gone the second time — but a caller
   * that sent one has miscounted something, and saying so is cheaper than letting it wonder later why its
   * numbers disagree.
   *
   * @param ids the ids the caller named, as it sent them
   * @return them; fails with `DuplicateDiscardId` when one repeats
   */
  private def distinct(ids: Chunk[String]): Validated[Chunk[String]] =
    if ids.distinct.size == ids.size then Validation.succeed(ids)
    else Validation.fail(InvalidInput.DuplicateDiscardId)


object QueueInputValidation:

  /**
   * The bounds the parse enforces, expressed where they are enforced.
   *
   * A domain type rather than a reach into the service's configuration: the rule *"a caller may not wait
   * longer than this"* belongs to the parse, while where the number comes from — a HOCON file, a flag, a
   * test — belongs to the adapter that provides it.
   *
   * @param maxWait the longest a caller may ask to wait for a message
   * @param maxBatchLimit the most messages one claim may take, whatever a caller asks for — the ceiling is
   *                      here so no single caller decides how much of a key's backlog it withholds from
   *                      everyone else
   */
  final case class Config(maxWait: Duration, maxBatchLimit: Int)
