package homelab.keyedqueue.domain.service.validation


import homelab.common.Validated
import homelab.keyedqueue.domain.error.InvalidInput
import homelab.keyedqueue.domain.model.lock.{ Demand, Claim }
import homelab.keyedqueue.domain.request.lock.{ AcquireRequest, RefreshRequest, ReleaseRequest, TryAcquireRequest }
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
   * The name must be present, the hold and the wait positive; both are clamped to the service's ceilings,
   * the wait as a dequeue's is. All three problems accumulate.
   *
   * @param request what the caller sent, untrusted
   * @return the demand to hand the store; accumulates `EmptyLockName`, `NonPositiveTtl` and
   *         `NonPositiveMaxWait`
   */
  def parse(request: AcquireRequest): Validated[Demand] =
    Validation
      .validate(
        nonEmptyString(request.name, InvalidInput.EmptyLockName).map(LockName.apply),
        holding(request.ttl),
        waiting(request.maxWait),
      )
      .map((name, ttl, patience) => Demand(name, ttl, patience))

  /**
   * Everything `TryAcquire` needs to be actionable.
   *
   * The name must be present and the hold positive, which is clamped to the service's ceiling. There is no
   * wait to check. Both problems accumulate.
   *
   * @param request what the caller sent, untrusted
   * @return the lock and how long to hold it; accumulates `EmptyLockName` and `NonPositiveTtl`
   */
  def parse(request: TryAcquireRequest): Validated[(LockName, Duration)] =
    Validation.validate(
      nonEmptyString(request.name, InvalidInput.EmptyLockName).map(LockName.apply),
      holding(request.ttl),
    )

  /**
   * The claim a `Release` names.
   *
   * @param request what the caller sent, untrusted
   * @return the claim; fails with `UnreadableReceipt` when the handle is not one this service issued
   */
  def parse(request: ReleaseRequest): Validated[Claim] =
    receipt(request.receipt)

  /**
   * The claim and hold length a `Refresh` names.
   *
   * @param request what the caller sent, untrusted
   * @return the claim and ttl; accumulates `UnreadableReceipt` and `NonPositiveTtl`
   */
  def parse(request: RefreshRequest): Validated[(Claim, Duration)] =
    Validation.validate(receipt(request.receipt), holding(request.ttl))

  /**
   * A receipt this service issued.
   *
   * @param value the handle as it arrived
   * @return the claim it names; fails with `UnreadableReceipt` when it names none
   */
  private def receipt(value: String): Validated[Claim] =
    Validation.fromOptionWith(InvalidInput.UnreadableReceipt)(Claim.decode(value))

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

  /**
   * How long this service will actually grant a hold for: positive, and clamped to the ceiling.
   *
   * The ceiling bounds how long a crashed holder blocks its lock — reclaim happens at lease expiry, so an
   * unbounded ttl would let one caller make that arbitrarily late. A holder that needs longer refreshes.
   *
   * @param asked how long the caller wants the hold to survive
   * @return that, or the ceiling, whichever is shorter, to the millisecond the store keeps leases in;
   *         fails with `NonPositiveTtl` when it is not positive
   */
  private def holding(asked: Duration): Validated[Duration] =
    if asked.toMillis <= 0 then Validation.fail(InvalidInput.NonPositiveTtl)
    else
      val granted = if asked > config.maxTtl then config.maxTtl else asked
      Validation.succeed(Duration.fromMillis(granted.toMillis))


object LockInputValidation:

  /**
   * The bounds the parse enforces, expressed where they are enforced.
   *
   * @param maxWait the longest a caller may ask to wait for a lock
   * @param maxTtl the longest a single grant's lease may run — refresh, not a long ttl, is how a hold lasts
   */
  final case class Config(maxWait: Duration, maxTtl: Duration)
