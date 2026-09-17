package homelab.keyedqueue.domain.service.usecase.lock


import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.common.orFail
import homelab.keyedqueue.domain.request.lock.RefreshRequest
import homelab.keyedqueue.domain.response.lock.RefreshResponse
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.validation.LockInputValidation
import zio.IO


/**
 * Extend a held lock's lease.
 *
 * @param store where the lock lives
 * @param validation what a well-formed refresh looks like
 */
final class LockRefreshUseCase(store: LockStore, validation: LockInputValidation):

  /**
   * Parse, then refresh.
   *
   * A lost hold comes back in the answer, so the caller learns to stop treating the lock as held.
   *
   * @param request the receipt and how much longer to hold, untrusted
   * @return the new lease, or that the hold is lost; aborts with `ValidationError` when the request is
   *         malformed, or with `ApplicationError` when the store fails
   */
  def apply(request: RefreshRequest): IO[ApplicationError, RefreshResponse] =
    validation.parse(request).orFail.flatMap {
      case (claim, ttl) =>
        store.refresh(claim, ttl).map {
          case (until, renewed) =>
            if renewed then RefreshResponse.Renewed(until) else RefreshResponse.Lost
        }
    }
