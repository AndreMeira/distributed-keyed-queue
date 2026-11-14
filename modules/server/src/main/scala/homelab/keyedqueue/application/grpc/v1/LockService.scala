package homelab.keyedqueue.application.grpc.v1


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.service.usecase.lock.LockUseCases
import homelab.keyedqueue.infrastructure.codecs.grpc.v1.Inbound.toDomain
import homelab.keyedqueue.infrastructure.codecs.grpc.v1.Outbound.toProto
import homelab.keyedqueue.v1
import homelab.keyedqueue.v1.ZioKeyedLockService
import zio.IO


/**
 * The gRPC surface for the lock: decode, call the use case, encode.
 *
 * No logic here beyond the codec, like [[QueueService]]. What is a fault and what is a result splits the
 * same way: a lock held until the wait elapsed is an ordinary answer (`acquired = false`), a lost hold is an
 * ordinary answer (`released`/`renewed = false`), and only a bad request or a broken store fails the call,
 * with the domain's own errors: [[Module]] turns them into a `Status` ([[errors.GrpcError]]). A malformed
 * request surfaces as a `ValidationError`, since the lock validates in the use case rather than the codec.
 *
 * `Acquire` hands back the numeric fence as well as the opaque receipt: the receipt releases and refreshes,
 * and the fence stamps writes to the protected resource so a stale holder's writes are rejected downstream.
 * See `docs/architecture/lock-guarantees.md`.
 *
 * @param monitor what each RPC is counted and timed against
 * @param useCases the lock's operations
 */
final class LockService(monitor: Monitor, useCases: LockUseCases) extends ZioKeyedLockService.GKeyedLock[Any, ApplicationError]:

  /**
   * Take the named lock, blocking up to `max_wait`.
   *
   * @param request the `v1.AcquireRequest`
   * @param context the call's context, unused
   * @return the `v1.AcquireResponse`; `acquired = false` when the wait elapsed; aborts with a
   *         `ValidationError` when the name is empty or a duration is not positive
   */
  override def acquire(request: v1.AcquireRequest, context: Any): IO[ApplicationError, v1.AcquireResponse] =
    monitor.measure("LockService.acquire"):
      for response <- useCases.acquire(request.toDomain)
      yield response.toProto

  /**
   * Take the named lock only if it is free right now.
   *
   * @param request the `v1.TryAcquireRequest`
   * @param context the call's context, unused
   * @return the `v1.AcquireResponse`; `acquired = false` when the lock is held or already queued for; aborts with
   *         a `ValidationError` when the name is empty or the ttl is not positive
   */
  override def tryAcquire(request: v1.TryAcquireRequest, context: Any): IO[ApplicationError, v1.AcquireResponse] =
    monitor.measure("LockService.tryAcquire"):
      for response <- useCases.tryAcquire(request.toDomain)
      yield response.toProto

  /**
   * Release a lock this caller holds.
   *
   * @param request the `v1.ReleaseRequest`
   * @param context the call's context, unused
   * @return the `v1.ReleaseResponse`; `released = false` when the hold had been revoked; aborts with
   *         a `ValidationError` when the receipt is not one this service issued
   */
  override def release(request: v1.ReleaseRequest, context: Any): IO[ApplicationError, v1.ReleaseResponse] =
    monitor.measure("LockService.release"):
      for response <- useCases.release(request.toDomain)
      yield response.toProto

  /**
   * Push a held lock's lease forward.
   *
   * @param request the `v1.RefreshRequest`
   * @param context the call's context, unused
   * @return the `v1.RefreshResponse`; `renewed = false` when the hold has been lost; aborts with
   *         a `ValidationError` when the receipt is not one this service issued or the ttl is not positive
   */
  override def refresh(request: v1.RefreshRequest, context: Any): IO[ApplicationError, v1.RefreshResponse] =
    monitor.measure("LockService.refresh"):
      for response <- useCases.refresh(request.toDomain)
      yield response.toProto
