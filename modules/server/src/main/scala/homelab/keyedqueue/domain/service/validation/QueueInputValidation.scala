package homelab.keyedqueue.domain.service.validation


import homelab.keyedqueue.domain.error.InvalidInput
import homelab.keyedqueue.domain.request.v1.{ DequeueRequest, EnqueueRequest, SettleRequest }
import homelab.keyedqueue.domain.syntax.Validated
import homelab.keyedqueue.domain.types.{ MessageId, MessageKey, QueueName }
import zio.Chunk
import zio.ZIO


/**
 * Format-level validation of what a caller sent: every problem with a request, in one pass.
 *
 * '''A class, though it holds nothing.''' It is wired as a dependency (see [[Module]]) rather than called as
 * an object, so a apply case declares that it validates, a test can substitute a validator that refuses
 * everything, and the day a check needs the store — is this queue known? is this key parked? — that check
 * arrives as a constructor parameter instead of a rewrite of every call site.
 *
 * '''A receipt is never validated here.''' `Settle` and `Heartbeat` carry receipts, and an unreadable one
 * is deliberately *not* an input error: settle reports it stale, heartbeat lists it among what the consumer
 * has lost. Both are documented on their apply cases. Validating them here would turn a result a caller must
 * handle into an error it must catch, which is the opposite of the API's shape.
 *
 * `Settle` still appears below, for the one field on it that *is* a format-level input: how many messages
 * behind this one to discard. `Heartbeat` carries nothing but receipts, so it has nothing to check.
 */
final class QueueInputValidation:

  /**
   * Everything `Enqueue` needs to be actionable.
   *
   * The checks run with `validate` rather than `zipRight`, so a request that names no queue *and* no key
   * comes back naming both. That is the only reason this is not a row of `if`s.
   *
   * The id is required because the store keys messages by it: two queued under one id for one key are one
   * message, and an empty id would collapse every unnamed message on that key into a single entry.
   *
   * @param request what the caller sent
   * @return noop; accumulates `EmptyQueueName`, `EmptyMessageKey` and `EmptyMessageId`
   */
  def parse(request: EnqueueRequest): Validated[Unit] =
    queue(request.queue)
      .validate(key(request.message.key))
      .validate(CommonValidation.nonEmpty(request.message.messageId, InvalidInput.EmptyMessageId))
      .unit

  /**
   * Everything `Dequeue` needs to be actionable.
   *
   * The queue, and that the batch size is not negative. How *much* a caller asks for — of patience or of
   * messages — is clamped rather than refused, because asking for more than the service offers is not a
   * mistake (see `SyncUseCases.Config`). Asking for a negative amount is.
   *
   * @param request what the caller sent
   * @return noop; accumulates `EmptyQueueName` and `NegativeMaxBatch`
   */
  def parse(request: DequeueRequest): Validated[Unit] =
    queue(request.queue)
      .validate(CommonValidation.nonNegative(request.maxBatch, InvalidInput.NegativeMaxBatch))
      .unit

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
   * @param request what the caller sent
   * @return noop; accumulates `EmptyDiscardId` and `DuplicateDiscardId`
   */
  def parse(request: SettleRequest): Validated[Unit] =
    val named = request.outcomes.map(_.messageId)
    ZIO
      .foreach(named)(id => CommonValidation.nonEmpty(id, InvalidInput.EmptyDiscardId))
      .validate(distinct(named))
      .unit

  /**
   * A queue must be named.
   *
   * @param value the name as it arrived
   * @return the name; fails with `EmptyQueueName`
   */
  private def queue(value: QueueName): Validated[QueueName] =
    CommonValidation.nonEmpty(value, InvalidInput.EmptyQueueName)

  /**
   * A message must be keyed.
   *
   * @param value the key as it arrived
   * @return the key; fails with `EmptyMessageKey`
   */
  private def key(value: MessageKey): Validated[MessageKey] =
    CommonValidation.nonEmpty(value, InvalidInput.EmptyMessageKey)

  /**
   * The same id must not be named twice in one discard.
   *
   * Harmless to the script — a second `LREM` for an id already gone finds nothing — but a caller that sent
   * one has miscounted something, and saying so is cheaper than letting it wonder why its numbers disagree.
   *
   * @param ids the ids the caller wants dropped
   * @return them; fails with `DuplicateDiscardId` when one repeats
   */
  private def distinct(ids: Chunk[MessageId]): Validated[Chunk[MessageId]] =
    if ids.distinct.size == ids.size then ZIO.succeed(ids) else ZIO.fail(InvalidInput.DuplicateDiscardId)
