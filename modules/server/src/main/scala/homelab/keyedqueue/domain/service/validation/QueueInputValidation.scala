package homelab.keyedqueue.domain.service.validation


import homelab.keyedqueue.domain.error.InvalidInput
import homelab.keyedqueue.domain.request.v1.{ DequeueRequest, EnqueueRequest, SettleRequest }
import homelab.keyedqueue.domain.syntax.Validated
import homelab.keyedqueue.domain.types.{ MessageKey, QueueName }
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
   * The two checks run with `validate` rather than `zipRight`, so a request that names no queue *and* no key
   * comes back naming both. That is the only reason this is not a pair of `if`s.
   *
   * @param request what the caller sent
   * @return noop; accumulates `EmptyQueueName` and `EmptyMessageKey`
   */
  def parse(request: EnqueueRequest): Validated[Unit] =
    queue(request.queue).validate(key(request.message.key)).unit

  /**
   * Everything `Dequeue` needs to be actionable.
   *
   * Only the queue: how long a caller is willing to wait is clamped rather than refused, because a caller
   * asking for more patience than the service offers has not made a mistake — see `SyncUseCases.Config`.
   *
   * @param request what the caller sent
   * @return noop; fails with `EmptyQueueName` when no queue is named
   */
  def parse(request: DequeueRequest): Validated[Unit] =
    queue(request.queue).unit

  /**
   * Everything `Settle` needs to be actionable.
   *
   * Only the discard count. The receipt is not checked here — see the note on this class — and the outcome
   * is an enum the wire layer has already refused if unspecified.
   *
   * @param request what the caller sent
   * @return noop; fails with `NegativeDiscardAhead` when the count is below zero
   */
  def parse(request: SettleRequest): Validated[Unit] =
    CommonValidation.nonNegative(request.discardAhead, InvalidInput.NegativeDiscardAhead).unit

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
