package homelab.keyedqueue.domain.service.usecase


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.ClaimRef
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.Verdict
import zio.{ Duration, IO, ZIO }


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
   * @param receipt the opaque handle from the delivery
   * @param verdict what the consumer decided
   * @param retryAfter how long to hold the key back before retrying; ignored for `Done`
   * @return true when applied, false when the claim was already gone; aborts with `QueueError` when the
   *         store fails
   */
  def apply(receipt: String, verdict: Verdict, retryAfter: Duration): IO[QueueError, Boolean] =
    ClaimRef.fromReceipt(receipt) match
      case None        => ZIO.succeed(false)
      case Some(claim) => store.settle(claim, verdict, retryAfter)
