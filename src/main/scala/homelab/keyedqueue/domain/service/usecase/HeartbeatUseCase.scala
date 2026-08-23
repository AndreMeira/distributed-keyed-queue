package homelab.keyedqueue.domain.service.usecase


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.ClaimRef
import homelab.keyedqueue.domain.request.queue.HeartbeatRequest
import homelab.keyedqueue.domain.response.queue.HeartbeatResponse
import homelab.keyedqueue.domain.service.persistence.QueueStore
import zio.IO


/**
 * Renew the claims a consumer still holds, and say which it has lost.
 *
 * @param store where the queue lives
 */
final class HeartbeatUseCase(store: QueueStore):

  /**
   * Renew everything named, and report the rest as stale.
   *
   * A receipt that cannot be decoded is reported stale rather than rejected: the consumer's obligation is
   * the same either way, and failing the whole call would cost it the claims that *are* still good.
   *
   * @param request everything the consumer believes it holds
   * @return the new deadline and what it no longer holds; aborts with `QueueError` when the store fails
   */
  def apply(request: HeartbeatRequest): IO[QueueError, HeartbeatResponse] =
    val decoded    = request.receipts.map(receipt => receipt -> ClaimRef.fromReceipt(receipt))
    val unreadable = decoded.collect { case (receipt, None) => receipt }
    val claims     = decoded.collect { case (_, Some(claim)) => claim }
    store.renew(claims).map((until, lost) => HeartbeatResponse(unreadable ++ lost.map(_.receipt), until))
