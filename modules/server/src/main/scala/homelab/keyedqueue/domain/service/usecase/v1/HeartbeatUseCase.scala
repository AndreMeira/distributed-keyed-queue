package homelab.keyedqueue.domain.service.usecase.v1


import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.keyedqueue.domain.model.{ Claim, Renewal }
import homelab.keyedqueue.domain.request.v1.QueueRequest
import homelab.keyedqueue.domain.response.v1.QueueResponse
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
   * the same either way, and failing the whole call would cost it the claims that *are* still good. That is
   * why the parse below is local and total, where enqueue, dequeue and settle are parsed by
   * `QueueInputValidation` and can be refused.
   *
   * Two kinds of loss come back as one list, deliberately. A receipt this service never issued and a claim
   * it has since revoked both mean "you do not hold this", and a consumer does the same thing about each.
   *
   * @param request everything the consumer believes it holds
   * @return the new deadline and what it no longer holds; aborts with `RedisFailure` when the store fails
   */
  def apply(request: QueueRequest.Heartbeat): IO[ApplicationError, QueueResponse.Heartbeat] =
    val renewal = parse(request)
    store.renew(renewal.held).map { (until, lost) =>
      QueueResponse.Heartbeat(renewal.unreadable ++ lost.map(_.reference), until)
    }

  /**
   * Sort what a consumer says it holds into what this service issued and what it did not.
   *
   * '''Total, and deliberately so.''' The parses in `QueueInputValidation` can refuse; this one cannot. A
   * heartbeat
   * carries many receipts, and one it cannot read says nothing about the others — refusing the call would
   * cost the consumer renewals that were good, to tell it something it learns anyway from the answer,
   * where the unreadable ones come back among what it has lost.
   *
   * The return type says it: `Renewal`, not `Validated[Renewal]`. A signature that promised failure it
   * could never deliver would leave every caller handling a case that does not exist.
   *
   * @param request what the caller sent, untrusted
   * @return its receipts, sorted
   */
  private def parse(request: QueueRequest.Heartbeat): Renewal =
    val read = request.receipts.map(receipt => receipt -> Claim.fromReference(receipt))
    Renewal(
      held = read.collect { case (_, Some(claim)) => claim },
      unreadable = read.collect { case (receipt, None) => receipt },
    )
