package homelab.keyedqueue.domain.service.validation


import homelab.common.Validated
import homelab.keyedqueue.domain.error.InvalidInput
import homelab.keyedqueue.domain.model.queue.*
import homelab.keyedqueue.domain.request.queue.{ DequeueRequest, EnqueueRequest, SettleRequest }
import homelab.keyedqueue.domain.request.lock.*
import homelab.keyedqueue.domain.service.validation.CommonValidation.{ nonNegative, nonEmpty as nonEmptyString }
import homelab.keyedqueue.domain.types.*
import zio.prelude.Validation
import zio.{ Chunk, Duration, NonEmptyChunk, duration2DurationOps }


/**
 * The crossing from what a caller sent to what this service will act on: every problem with a request, in
 * one pass.
 *
 * Each method answers with the type the store accepts, so the only way to hold one is to have come through
 * here. A receipt is read for shape only: whether the claim it names is still held is answered by the store
 * and reported as `Stale`.
 *
 * `Heartbeat` is parsed by `HeartbeatUseCase`, where an unreadable receipt is an answer rather than an error.
 */
final class QueueInputValidation(config: QueueInputValidation.Config):

  /**
   * Everything `Enqueue` needs to be actionable.
   *
   * @param request what the caller sent, untrusted
   * @return the submission to hand the store; accumulates `EmptyQueueName`, `EmptyMessageKey` and
   *         `EmptyMessageId`
   */
  def parse(request: EnqueueRequest): Validated[Submission] =
    Validation
      .validate(nonEmptyQueueName(request.queue), message(request.message))
      .map(Submission.apply)

  /**
   * Everything `Dequeue` needs to be actionable.
   *
   * Asking for more patience or more messages than the service offers is clamped; asking for a negative
   * batch, or for no patience at all, is refused.
   *
   * @param request what the caller sent, untrusted
   * @return the demand to hand the store, bounded; accumulates `EmptyQueueName`, `NonPositiveMaxWait` and
   *         `NegativeMaxBatch`
   */
  def parse(request: DequeueRequest): Validated[Demand] =
    Validation
      .validate(
        nonEmptyQueueName(request.queue),
        positiveWaitingTime(request.maxWait),
        nonNegative(request.maxBatch, InvalidInput.NegativeMaxBatch),
      )
      .map((name, wait, batch) => Demand(name, wait, batchSize(batch)))

  /**
   * Everything `Settle` needs to be actionable.
   *
   * Only the ids named. Whether the claim owns them is not checked here — the store looks them up under the
   * claim, and one it does not own is ignored.
   *
   * @param request what the caller sent, untrusted
   * @return the settlement to hand the store; accumulates `UnreadableReceipt`, `EmptySettle`,
   *         `EmptyDiscardId` and `DuplicateDiscardId`
   */
  def parse(request: SettleRequest): Validated[Settlement] =
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
   * Unreadable and revoked are different failures: this answers the first, the store's fence the second.
   *
   * @param value the receipt as it arrived
   * @return the claim it names; fails with `UnreadableReceipt` when it names none
   */
  private def receipt(value: String): Validated[Claim] =
    Claim.decode(value) match
      case Some(claim) => Validation.succeed(claim)
      case None        => Validation.fail(InvalidInput.UnreadableReceipt)

  /**
   * The messages a settle names, parsed, and known to be at least one.
   *

   * @param outcomes what the caller sent
   * @return them in domain terms, non-empty; fails with `EmptySettle` when there are none, or accumulates
   *         `EmptyDiscardId` for each that names nothing
   */
  private def batch(outcomes: Chunk[SettleRequest.MessageOutcome]): Validated[NonEmptyChunk[Settlement.Outcome]] =
    NonEmptyChunk.fromChunk(outcomes) match
      case Some(named) => Validation.validateAll(named.map(outcome))
      case None        => Validation.fail(InvalidInput.EmptySettle)

  /**
   * One named message becomes one the store can address.
   *
   * @param outcome what the caller said about it
   * @return the outcome in domain terms; fails with `EmptyDiscardId` when it names nothing
   */
  private def outcome(outcome: SettleRequest.MessageOutcome): Validated[Settlement.Outcome] =
    nonEmptyString(outcome.messageId, InvalidInput.EmptyDiscardId)
      .map(id => Settlement.Outcome(MessageId(id), outcome.outcome))

  /**
   * A zero backoff is no backoff.
   *

   * @param retryAfter what the caller asked for
   * @return the wait, or `None` when none was asked for
   */
  private def backoff(retryAfter: Duration): Option[Duration] =
    Option.when(retryAfter.toMillis > 0)(retryAfter)

  /**
   * How long this service will actually wait, once it is satisfied the caller means to wait at all.
   *
   * Longer than the ceiling is clamped; none at all is refused.
   *
   * @param asked what the caller is prepared to wait
   * @return that, or the service's ceiling, whichever is shorter; fails with `NonPositiveMaxWait` when the
   *         caller asked to wait no time at all
   */
  private def positiveWaitingTime(asked: Duration): Validated[Duration] =
    if asked.toMillis <= 0 then Validation.fail(InvalidInput.NonPositiveMaxWait)
    else Validation.succeed(if asked > config.maxWait then config.maxWait else asked)

  /**
   * How many messages this claim may take.
   *
   * Zero and one both mean one message, so the floor is one rather than a refusal.
   *
   * @param asked how many the caller wants, already known not to be negative
   * @return that, bounded to one at the bottom and the service's limit at the top
   */
  private def batchSize(asked: Int): Int =
    asked.max(1).min(config.maxBatchLimit)

  /**
   * A queue must be named, and naming it is what makes it a [[QueueName]].
   *
   * @param value the name as it arrived
   * @return the name; fails with `EmptyQueueName`
   */
  private def nonEmptyQueueName(value: String): Validated[QueueName] =
    nonEmptyString(value, InvalidInput.EmptyQueueName).map(QueueName.apply)

  /**
   * A message the store can file: keyed, and addressable by its own name.
   *
   * The three checks accumulate, so a message missing all of them comes back saying all three. Everything
   * else crosses unchanged — the payload is cargo, and the queue reads neither it nor the encoding.
   *
   * `payloadType` is deliberately not checked here, matching what this service did before the parse
   * existed. An empty one is storable and meaningless, which is worth refusing one day; doing it now would
   * hide a contract change inside a refactor.
   *
   * @param message the message as it arrived
   * @return it in domain terms; accumulates `EmptyMessageKey`, `EmptyMessageId` and `EmptyEncoding`
   */
  private def message(message: EnqueueRequest.Message): Validated[Message] =
    Validation
      .validate(
        nonEmptyString(message.key, InvalidInput.EmptyMessageKey).map(MessageKey.apply),
        nonEmptyString(message.messageId, InvalidInput.EmptyMessageId).map(MessageId.apply),
        nonEmptyString(message.encoding, InvalidInput.EmptyEncoding),
      )
      .map((key, id, encoding) => Message(key, id, message.payloadType, encoding, message.sentAt, message.payload))

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
