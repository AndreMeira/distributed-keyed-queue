package homelab.keyedqueue.domain.service.usecase.v1


import homelab.common.orFail
import homelab.common.error.{ApplicationError, ValidationError}
import homelab.keyedqueue.domain.request.queue.SettleRequest
import homelab.keyedqueue.domain.request.lock.*
import homelab.keyedqueue.domain.response.v1.*
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import homelab.keyedqueue.domain.response.v1.SettleResponse.Applied
import zio.IO


/**
 * Report what a consumer did with a message.
 *
 * @param store where the queue lives
 * @param validation what a well-formed settle looks like
 */
final class SettleUseCase(store: QueueStore, validation: QueueInputValidation):

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
    validation.parse(request).orFail.flatMap { settlement =>
      store.settle(settlement).map { applied =>
        SettleResponse(if applied then Applied.Ok else Applied.Stale)
      }
    }
