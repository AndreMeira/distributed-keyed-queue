package homelab.keyedqueue.domain.service.validation


import homelab.common.Validated
import homelab.keyedqueue.domain.error.InvalidInput
import homelab.keyedqueue.domain.model.{ Acquisition, LockClaim }
import homelab.keyedqueue.domain.request.v1.{ AcquireRequest, RefreshRequest, ReleaseRequest }
import homelab.keyedqueue.domain.service.validation.CommonValidation.nonEmpty as nonEmptyString
import homelab.keyedqueue.domain.types.*
import zio.prelude.Validation
import zio.{ Duration, duration2DurationOps }


/**
 * The crossing from what a lock caller sent to what the store will act on — the lock's
 * [[QueueInputValidation]].
 *
 * Each method answers with the type the store accepts, so the only way to hold one is to have come through
 * here. A receipt is read for shape only: whether the claim it names is still held is answered by the store.
 *
 * @param config the bounds the parse enforces
 */
final class LockInputValidation(config: LockInputValidation.Config):

  /**
   * Everything `Acquire` needs to be actionable.
   *
   * The name must be present, the hold and the wait positive; the wait is clamped to the service's ceiling,
   * as a dequeue's is. All three problems accumulate.
   *
   * @param request what the caller sent, untrusted
   * @return the acquisition to hand the store; accumulates `EmptyLockName`, `NonPositiveTtl` and
   *         `NonPositiveMaxWait`
   */
  def parse(request: AcquireRequest): Validated[Acquisition] =
    Validation
      .validate(
        nonEmptyString(request.name, InvalidInput.EmptyLockName).map(LockName.apply),
        positive(request.ttl, InvalidInput.NonPositiveTtl),
        waiting(request.maxWait),
      )
      .map((name, ttl, patience) => Acquisition(name, ttl, patience))

  /**
   * The claim a `Release` names.
   *
   * @param request what the caller sent, untrusted
   * @return the claim; fails with `UnreadableReceipt` when the handle is not one this service issued
   */
  def parse(request: ReleaseRequest): Validated[LockClaim] =
    receipt(request.receipt)

  /**
   * The claim and hold length a `Refresh` names.
   *
   * @param request what the caller sent, untrusted
   * @return the claim and ttl; accumulates `UnreadableReceipt` and `NonPositiveTtl`
   */
  def parse(request: RefreshRequest): Validated[(LockClaim, Duration)] =
    Validation.validate(receipt(request.receipt), positive(request.ttl, InvalidInput.NonPositiveTtl))

  /**
   * A receipt this service issued.
   *
   * @param value the handle as it arrived
   * @return the claim it names; fails with `UnreadableReceipt` when it names none
   */
  private def receipt(value: String): Validated[LockClaim] =
    Validation.fromOptionWith(InvalidInput.UnreadableReceipt)(LockClaim.fromReceipt(value))

  /**
   * A duration that is positive.
   *
   * @param value what the caller asked for
   * @param problem what to report when it is not positive
   * @return the duration; fails with `problem` when it is absent or not positive
   */
  private def positive(value: Duration, problem: InvalidInput): Validated[Duration] =
    if value.toMillis <= 0 then Validation.fail(problem) else Validation.succeed(value)

  /**
   * How long this service will actually wait for a lock: positive, and clamped to the ceiling.
   *
   * @param asked what the caller is prepared to wait
   * @return that, or the ceiling, whichever is shorter; fails with `NonPositiveMaxWait` when it is not
   *         positive
   */
  private def waiting(asked: Duration): Validated[Duration] =
    if asked.toMillis <= 0 then Validation.fail(InvalidInput.NonPositiveMaxWait)
    else Validation.succeed(if asked > config.maxWait then config.maxWait else asked)


object LockInputValidation:

  /**
   * The bounds the parse enforces, expressed where they are enforced.
   *
   * @param maxWait the longest a caller may ask to wait for a lock
   */
  final case class Config(maxWait: Duration)
