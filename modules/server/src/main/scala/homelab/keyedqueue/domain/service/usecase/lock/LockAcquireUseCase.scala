package homelab.keyedqueue.domain.service.usecase.lock

import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.common.orFail
import homelab.keyedqueue.domain.request.lock.AcquireRequest
import homelab.keyedqueue.domain.response.lock.AcquireResponse
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.validation.LockInputValidation
import zio.IO


/**
 * Take a lock, blocking until it is free or the wait elapses.
 *
 * @param store where the lock lives
 * @param validation what turns a request into an acquisition this service will honour
 */
final class LockAcquireUseCase(store: LockStore, validation: LockInputValidation):

  /**
   * Parse, then acquire.
   *
   * Nothing acquired in time is a *response*, not a failure: a caller that waited its patience for a held
   * lock has behaved exactly as asked.
   *
   * @param request the lock and what the caller is asking for, untrusted
   * @return the grant, or nothing when the wait elapsed; aborts with `ValidationError` when the request is
   *         malformed, or with `ApplicationError` when the store fails
   */
  def apply(request: AcquireRequest): IO[ApplicationError, AcquireResponse] =
    validation.parse(request).orFail.flatMap { acquisition =>
      store.acquire(acquisition).map {
        case Some(hold) => AcquireResponse.Granted(hold.claim, hold.leaseUntil)
        case None       => AcquireResponse.Unavailable
      }
    }
