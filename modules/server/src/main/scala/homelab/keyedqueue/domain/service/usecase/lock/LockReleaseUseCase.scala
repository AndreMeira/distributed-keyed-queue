package homelab.keyedqueue.domain.service.usecase.lock


import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.common.orFail
import homelab.keyedqueue.domain.request.lock.ReleaseRequest
import homelab.keyedqueue.domain.response.lock.ReleaseResponse
import homelab.keyedqueue.domain.service.persistence.LockStore
import homelab.keyedqueue.domain.service.validation.LockInputValidation
import zio.IO


/**
 * Release a lock the caller holds.
 *
 * @param store where the lock lives
 * @param validation what a well-formed release looks like
 */
final class LockReleaseUseCase(store: LockStore, validation: LockInputValidation):

  /**
   * Parse the receipt, then release.
   *
   * `false` says the hold had already been revoked, which the caller must be told so it stops acting as
   * the holder.
   *
   * @param request the receipt, untrusted
   * @return whether it applied; aborts with `ValidationError` when the receipt is not one this service
   *         issued, or with `ApplicationError` when the store fails
   */
  def apply(request: ReleaseRequest): IO[ApplicationError, ReleaseResponse] =
    validation.parse(request).orFail.flatMap(claim => store.release(claim).map(ReleaseResponse.apply))
