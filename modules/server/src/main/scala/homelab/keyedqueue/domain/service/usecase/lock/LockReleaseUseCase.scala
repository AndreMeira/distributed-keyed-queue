package homelab.keyedqueue.domain.service.usecase.queue


import homelab.common.orFail
import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.keyedqueue.domain.request.lock.ReleaseRequest
import homelab.keyedqueue.domain.response.v1.ReleaseResponse
import homelab.keyedqueue.domain.service.lock.LockStore
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
   * `false` is a response, not a failure: the hold had already been revoked, which the caller must be told
   * rather than have raised at it.
   *
   * @param request the receipt, untrusted
   * @return whether it applied; aborts with `ValidationError` when the receipt is not one this service
   *         issued, or with `ApplicationError` when the store fails
   */
  def apply(request: ReleaseRequest): IO[ApplicationError, ReleaseResponse] =
    validation.parse(request).orFail.flatMap(claim => store.release(claim).map(ReleaseResponse.apply))
