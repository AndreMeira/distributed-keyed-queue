package homelab.keyedqueue.domain.service.usecase.v1


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.Claim
import homelab.keyedqueue.domain.request.v1.SettleRequest
import homelab.keyedqueue.domain.response.v1.SettleResponse
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import homelab.keyedqueue.domain.syntax.*
import homelab.keyedqueue.domain.types.Applied
import zio.{ IO, ZIO }


/**
 * Report what a consumer did with a message.
 *
 * @param store where the queue lives
 * @param validation what a well-formed settle looks like
 */
final class SettleUseCase(store: QueueStore, validation: QueueInputValidation):

  /**
   * Apply the verdict, if the claim is still the caller's.
   *
   * A receipt that cannot be decoded is treated exactly like one whose claim was revoked. The distinction
   * would tell a caller whether it was confused or merely late, and neither changes what it must do next:
   * stop, and do not touch that key.
   *
   * @param request the receipt, the verdict, any backoff, and how much to discard behind it
   * @return whether it applied; aborts with `InvalidRequest` when the discard count is negative, or with
   *         `QueueError` when the store fails
   */
  def apply(request: SettleRequest): IO[QueueError, SettleResponse] =
    validation.parse(request).orFail *> settle(request)

  /**
   * Apply a settle already known to be well formed.
   *
   * @param request what the caller sent
   * @return whether it applied; aborts with `QueueError` when the store fails
   */
  private def settle(request: SettleRequest): IO[QueueError, SettleResponse] =
    Claim.fromReference(request.receipt) match
      case None        => ZIO.succeed(SettleResponse(Applied.Stale))
      case Some(claim) =>
        store
          .settle(claim, request.outcome, request.retryAfter, request.discardAhead)
          .map(applied => SettleResponse(if applied then Applied.Ok else Applied.Stale))
