package homelab.keyedqueue.domain.service.usecase.v1


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.request.v1.QueueRequest
import homelab.keyedqueue.domain.response.v1.QueueResponse
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import zio.IO


/**
 * Renew the claims a consumer still holds, and say which it has lost.
 *
 * @param store where the queue lives
 * @param validation what sorts the receipts a consumer sent
 */
final class HeartbeatUseCase(store: QueueStore, validation: QueueInputValidation):

  /**
   * Renew everything named, and report the rest as stale.
   *
   * A receipt that cannot be decoded is reported stale rather than rejected: the consumer's obligation is
   * the same either way, and failing the whole call would cost it the claims that *are* still good. That
   * is why this parse is the one that cannot fail — see [[QueueInputValidation.parse]] for `Heartbeat`.
   *
   * Two kinds of loss come back as one list, deliberately. A receipt this service never issued and a claim
   * it has since revoked both mean "you do not hold this", and a consumer does the same thing about each.
   *
   * @param request everything the consumer believes it holds
   * @return the new deadline and what it no longer holds; aborts with `QueueError` when the store fails
   */
  def apply(request: QueueRequest.Heartbeat): IO[QueueError, QueueResponse.Heartbeat] =
    val renewal = validation.parse(request)
    store.renew(renewal.held).map { (until, lost) =>
      QueueResponse.Heartbeat(renewal.unreadable ++ lost.map(_.reference), until)
    }
