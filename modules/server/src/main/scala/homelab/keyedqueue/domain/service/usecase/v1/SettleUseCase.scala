package homelab.keyedqueue.domain.service.usecase.v1


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.Claim
import homelab.keyedqueue.domain.request.v1.SettleRequest
import homelab.keyedqueue.domain.response.v1.SettleResponse
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.Applied
import zio.{ IO, ZIO }


/**
 * Report what a consumer did with a message.
 *
 * @param store where the queue lives
 */
final class SettleUseCase(store: QueueStore):

  /**
   * Apply the verdict, if the claim is still the caller's.
   *
   * A receipt that cannot be decoded is treated exactly like one whose claim was revoked. The distinction
   * would tell a caller whether it was confused or merely late, and neither changes what it must do next:
   * stop, and do not touch that key.
   *
   * @param request the receipt, the verdict, and any backoff
   * @return whether it applied; aborts with `QueueError` when the store fails
   */
  def apply(request: SettleRequest): IO[QueueError, SettleResponse] =
    Claim.fromReference(request.receipt) match
      case None        => ZIO.succeed(SettleResponse(Applied.Stale))
      case Some(claim) =>
        store
          .settle(claim, request.outcome, request.retryAfter)
          .map(applied => SettleResponse(if applied then Applied.Ok else Applied.Stale))
