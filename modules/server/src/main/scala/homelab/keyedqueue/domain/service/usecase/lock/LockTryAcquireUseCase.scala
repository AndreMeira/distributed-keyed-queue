package homelab.keyedqueue.domain.service.usecase.lock


import homelab.common.error.ApplicationError
import homelab.common.orFail
import homelab.keyedqueue.domain.request.lock.TryAcquireRequest
import homelab.keyedqueue.domain.response.lock.AcquireResponse
import homelab.keyedqueue.domain.service.persistence.LockStore
import homelab.keyedqueue.domain.service.validation.LockInputValidation
import zio.*


/**
 * Take a lock if it is free this instant, and say so either way.
 *
 * The non-waiting half of the lock API: no ticket is taken and no readiness is involved, so a caller that
 * cannot have the lock now learns it in one round trip.
 *
 * @param store where the lock lives
 * @param validation what turns the request into the lock and hold it names
 */
final class LockTryAcquireUseCase(store: LockStore, validation: LockInputValidation):

  /**
   * Parse, then take the lock if nobody holds it and nobody is queued for it.
   *
   * @param request the lock and hold the caller asks for, untrusted
   * @return the grant, or `Unavailable` when the lock is held or already queued for; aborts with
   *         `ValidationError` when the request is malformed, or with `ApplicationError` when the store fails
   */
  def apply(request: TryAcquireRequest): IO[ApplicationError, AcquireResponse] =
    for
      (name, ttl) <- validation.parse(request).orFail
      held        <- store.tryAcquire(name, ttl)
    yield held match
      case Some(hold) => AcquireResponse.Granted(hold.claim.reference, hold.claim.token, hold.leaseUntil, ttl)
      case None       => AcquireResponse.Unavailable
