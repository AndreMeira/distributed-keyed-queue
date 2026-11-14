package homelab.keyedqueue.domain.service.usecase.queue


import homelab.common.orFail
import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.keyedqueue.domain.model.queue.Settlement
import homelab.keyedqueue.domain.request.queue.SettleRequest
import homelab.keyedqueue.domain.request.lock.*
import homelab.keyedqueue.domain.response.queue.*
import homelab.keyedqueue.domain.service.maintenance.DueTimer
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import homelab.keyedqueue.domain.response.queue.SettleResponse.Applied
import zio.{ Clock, Duration, IO, UIO, ZIO }


/**
 * Report what a consumer did with a message.
 *
 * @param store where the queue lives
 * @param validation what a well-formed settle looks like
 * @param timer asked for a tick when a nack's backoff ends, so this instance releases its key on time
 */
final class SettleUseCase(store: QueueStore, validation: QueueInputValidation, timer: DueTimer):

  /**
   * Apply the outcomes, if the claim is still the caller's.
   *
   * Nothing is decided here: `Settlement` is obtainable only from [[QueueInputValidation.parse]], so what
   * is left is one call and the reading of its answer.
   *
   * Two failures a caller might confuse are kept apart. A string that was never a receipt is refused as
   * a `ValidationError`: nothing issued it, and no retry makes it valid. A receipt whose claim has since been
   * revoked is answered `Stale`, because that is a race a correct consumer can lose — it was merely late.
   *
   * @param request the receipt, what became of which message, and any backoff
   * @return whether it applied; aborts with `ValidationError` when the receipt is not one this service
   *         issued, or the request names no messages, an empty id, or the same id twice — or with
   *         `ApplicationError` when the store fails
   */
  def apply(request: SettleRequest): IO[ApplicationError, SettleResponse] =
    for
      settlement <- validation.parse(request).orFail
      applied    <- record(settlement)
    yield SettleResponse(if applied then Applied.Ok else Applied.Stale)

  /**
   * Apply the settlement, then, when it asked for a backoff, ask the timer for a tick when the backoff ends: one step
   * a cancellation cannot split.
   *
   * Uninterruptible for the reason the schedule use case is: a caller cancelled between the two would leave a key
   * held back with no tick asked for, released only by the next pass. The tick is asked for the backoff less the
   * store call's round trip, which can only make it early, and an early tick finds nothing due. A backoff on a
   * settle that nacked nothing asks for a tick that finds nothing either, which is harmless.
   *
   * @param settlement the claim, what became of its messages, and any backoff
   * @return whether it applied; aborts with `ApplicationError` when the store fails
   */
  private def record(settlement: Settlement): IO[ApplicationError, Boolean] =
    ZIO.uninterruptible:
      for
        sent     <- Clock.instant
        applied  <- store.settle(settlement)
        answered <- Clock.instant
        _        <- backoff(settlement.retryAfter, applied, Duration.fromInterval(sent, answered))
      yield applied

  /**
   * Ask for a tick when the backoff ends, if there is one and the settle applied.
   *
   * @param retryAfter the backoff the settle asked for, if any
   * @param applied whether the claim was still the caller's, so the settle took effect
   * @param elapsed how long the store call took
   * @return noop once asked, or at once when there is nothing to ask for
   */
  private def backoff(retryAfter: Option[Duration], applied: Boolean, elapsed: Duration): UIO[Unit] =
    retryAfter match
      case Some(wait) if applied => timer.triggerIn(wait minus elapsed)
      case _                     => ZIO.unit
