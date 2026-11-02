package homelab.keyedqueue.domain.service.usecase.lock


import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import homelab.common.orFail
import homelab.keyedqueue.domain.model.lock.Hold
import homelab.keyedqueue.domain.request.lock.TryAcquireRequest
import homelab.keyedqueue.domain.response.lock.AcquireResponse
import homelab.keyedqueue.domain.service.persistence.LockStore
import homelab.keyedqueue.domain.service.validation.LockInputValidation
import homelab.keyedqueue.domain.types.LockName
import zio.*


/**
 * Take a lock if it is free this instant, and say so either way.
 *
 * The non-waiting half of the lock API: no ticket is taken and no readiness is involved, so a caller that
 * cannot have the lock now learns it in one round trip.
 *
 * The call is a scope, and the ask that can grant is acquired in it: a call that does not answer after the
 * store granted, because it was cancelled, releases the hold rather than leaving it to block everyone until
 * its lease expires. A release that does not land is left alone, and the hold lapses on its lease, as it
 * would for a holder that died. A call cancelled after it has answered is beyond this: the response is lost
 * on its way out, and the hold lapses on its lease.
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
    ZIO.scoped:
      for
        (name, ttl) <- validation.parse(request).orFail
        held        <- tryAcquire(name, ttl)
      yield held match
        case Some(hold) => AcquireResponse.Granted(hold.claim.reference, hold.claim.token, hold.leaseUntil, ttl)
        case None       => AcquireResponse.Unavailable

  /**
   * Take the lock if it is free this instant, as a resource of the call's scope: when the call ends without
   * answering, a grant is released.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives without a refresh
   * @return the hold, or `None` when the lock is held or queued for; aborts with an `AdapterError` when the
   *         store fails
   */
  private def tryAcquire(name: LockName, ttl: Duration): ZIO[Scope, AdapterError, Option[Hold]] =
    ZIO.acquireReleaseExit(store.tryAcquire(name, ttl)):
      case Some(hold) -> Exit.Failure(_) => store.release(hold.claim).ignore
      case _                             => ZIO.unit
