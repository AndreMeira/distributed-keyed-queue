package homelab.keyedqueue.domain.service.usecase.v1


import homelab.common.orFail
import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.keyedqueue.domain.request.v1.QueueRequest
import homelab.keyedqueue.domain.response.v1.QueueResponse
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import homelab.keyedqueue.domain.types.Applied
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
   * '''Nothing is decided here.''' Reading the receipt is a parse, so it belongs with the other parses —
   * `Settlement` is obtainable only from [[QueueInputValidation.parse]], and this use case has no way to
   * reach past it into the untrusted request. What is left is one call and the reading of its answer.
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
  def apply(request: QueueRequest.Settle): IO[ApplicationError, QueueResponse.Settle] =
    validation.parse(request).orFail.flatMap { settlement =>
      store.settle(settlement).map { applied =>
        QueueResponse.Settle(if applied then Applied.Ok else Applied.Stale)
      }
    }
