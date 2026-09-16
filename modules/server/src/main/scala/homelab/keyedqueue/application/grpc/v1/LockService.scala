package homelab.keyedqueue.application.grpc.v1


import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.service.usecase.v1.SyncLockUseCases
import homelab.keyedqueue.infrastructure.codecs.grpc.v1.Inbound.toDomain
import homelab.keyedqueue.infrastructure.codecs.grpc.v1.Outbound.toProto
import homelab.keyedqueue.v1
import homelab.keyedqueue.v1.ZioKeyedLockService
import io.grpc.{ Status, StatusException }
import zio.IO


/**
 * The gRPC surface for the lock: decode, call the use case, encode.
 *
 * No logic here beyond the codec, like [[QueueService]]. What is a fault and what is a result splits the
 * same way: a lock held until the wait elapsed is an ordinary answer (`acquired = false`), a lost hold is an
 * ordinary answer (`released`/`renewed = false`), and only a bad request or a broken store becomes a
 * `Status`.
 *
 * `Acquire` hands back the numeric fence as well as the opaque receipt: the receipt releases and refreshes,
 * and the fence stamps writes to the protected resource so a stale holder's writes are rejected downstream.
 * See `docs/architecture/lock-guarantees.md`.
 *
 * @param monitor what each RPC is counted and timed against
 * @param useCases the lock's operations
 */
final class LockService(monitor: Monitor, useCases: SyncLockUseCases) extends ZioKeyedLockService.KeyedLock:

  /**
   * Take the named lock, blocking up to `max_wait`.
   *
   * @param request the wire request
   * @return the wire response; `acquired = false` when the wait elapsed; aborts with `INVALID_ARGUMENT`
   *         when the name is empty or a duration is not positive
   */
  override def acquire(request: v1.AcquireRequest): IO[StatusException, v1.AcquireResponse] =
    monitor.measure("LockService.acquire"):
      useCases.acquire(request.toDomain).mapBoth(status, _.toProto)

  /**
   * Release a lock this caller holds.
   *
   * @param request the wire request
   * @return the wire response; `released = false` when the hold had been revoked; aborts with
   *         `INVALID_ARGUMENT` when the receipt is not one this service issued
   */
  override def release(request: v1.ReleaseRequest): IO[StatusException, v1.ReleaseResponse] =
    monitor.measure("LockService.release"):
      useCases.release(request.toDomain).mapBoth(status, _.toProto)

  /**
   * Push a held lock's lease forward.
   *
   * @param request the wire request
   * @return the wire response; `renewed = false` when the hold has been lost; aborts with
   *         `INVALID_ARGUMENT` when the receipt is not one this service issued or the ttl is not positive
   */
  override def refresh(request: v1.RefreshRequest): IO[StatusException, v1.RefreshResponse] =
    monitor.measure("LockService.refresh"):
      useCases.refresh(request.toDomain).mapBoth(status, _.toProto)

  /**
   * Map a use-case failure to a status — by the toolkit's markers, as [[QueueService]] does. A malformed
   * request surfaces here as a `ValidationError`, since the lock validates in the use case rather than the
   * codec.
   *
   * @param error what went wrong
   * @return the status
   */
  private def status(error: ApplicationError): StatusException = error match
    case invalid: ValidationError           => StatusException(Status.INVALID_ARGUMENT.withDescription(invalid.message))
    case _: ApplicationError.TransientError => StatusException(Status.UNAVAILABLE.withDescription(error.message))
    case _                                  => StatusException(Status.INTERNAL.withDescription(""))
