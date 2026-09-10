package homelab.keyedqueue.application.grpc.v1


import com.google.protobuf.duration.Duration as WireDuration
import com.google.protobuf.timestamp.Timestamp
import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.lock.LockStore.{ Hold, LockReceipt }
import homelab.keyedqueue.v1
import homelab.keyedqueue.v1.ZioKeyedLockService
import io.grpc.{ Status, StatusException }
import zio.*

import java.time.Instant


/**
 * The gRPC surface for the lock: decode, call the store, encode.
 *
 * No logic here beyond the parse, like [[QueueService]]. What is a fault and what is a result splits the
 * same way: a lock held until the wait elapsed is an ordinary answer (`acquired = false`), a lost hold is
 * an ordinary answer (`released`/`renewed = false`), and only a bad request or a broken store becomes a
 * `Status`.
 *
 * '''The fence is returned, on purpose.''' `Acquire` hands back the numeric token as well as the opaque
 * receipt: the receipt is for releasing and refreshing, the fence is for stamping writes to the protected
 * resource so a stale holder's writes are rejected downstream (docs/research/distributed-lock.md). This is
 * the lock's public contract, unlike the queue where the fence stays internal.
 *
 * @param monitor what each RPC is counted and timed against
 * @param store the lock this surface exposes
 */
final class LockService(monitor: Monitor, store: LockStore) extends ZioKeyedLockService.KeyedLock:

  /**
   * Take the named lock, blocking up to `max_wait`.
   *
   * @param request the wire request
   * @return the wire response; `acquired = false` when the wait elapsed; aborts with `INVALID_ARGUMENT`
   *         when the name is empty or a duration is not positive
   */
  override def acquire(request: v1.AcquireRequest): IO[StatusException, v1.AcquireResponse] =
    monitor.measure("LockService.acquire"):
      for
        name    <- named(request.name)
        ttl     <- positive(request.ttl, "ttl")
        wait    <- positive(request.maxWait, "max_wait")
        outcome <- store.acquire(name, ttl, wait).mapError(status)
      yield outcome match
        case Some(hold) =>
          v1.AcquireResponse(acquired = true, hold.receipt, hold.token, Some(stamp(hold.leaseUntil)))
        case None       =>
          v1.AcquireResponse(acquired = false)

  /**
   * Release a lock this caller holds.
   *
   * @param request the wire request
   * @return the wire response; `released = false` when the hold had been revoked; aborts with
   *         `INVALID_ARGUMENT` when the receipt is not one this service issued
   */
  override def release(request: v1.ReleaseRequest): IO[StatusException, v1.ReleaseResponse] =
    monitor.measure("LockService.release"):
      for
        (name, token) <- receipt(request.receipt)
        released      <- store.release(name, token).mapError(status)
      yield v1.ReleaseResponse(released)

  /**
   * Push a held lock's lease forward.
   *
   * @param request the wire request
   * @return the wire response; `renewed = false` when the hold has been lost; aborts with
   *         `INVALID_ARGUMENT` when the receipt is not one this service issued or the ttl is not positive
   */
  override def refresh(request: v1.RefreshRequest): IO[StatusException, v1.RefreshResponse] =
    monitor.measure("LockService.refresh"):
      for
        (name, token)    <- receipt(request.receipt)
        ttl              <- positive(request.ttl, "ttl")
        (until, renewed) <- store.refresh(name, token, ttl).mapError(status)
      yield v1.RefreshResponse(renewed, Option.when(renewed)(stamp(until)))

  /**
   * A lock name that is present.
   *
   * @param name the requested name
   * @return the name; aborts with `INVALID_ARGUMENT` when it is empty
   */
  private def named(name: String): IO[StatusException, String] =
    ZIO.cond(name.nonEmpty, name, reject("a lock name is required"))

  /**
   * A duration that is positive — the same rule Dequeue's `max_wait` obeys: an absent one reads as zero and
   * a lock that will not wait, or lives no time, is refused rather than inferred.
   *
   * @param duration the wire duration, absent when unset
   * @param field what to name in the rejection
   * @return the duration; aborts with `INVALID_ARGUMENT` when it is absent or not positive
   */
  private def positive(duration: Option[WireDuration], field: String): IO[StatusException, Duration] =
    val millis = duration.fold(0L)(d => d.seconds * 1000 + d.nanos / 1_000_000)
    ZIO.cond(millis > 0, Duration.fromMillis(millis), reject(s"$field is required and must be greater than zero"))

  /**
   * The name and token a receipt names.
   *
   * @param value the opaque receipt from an acquire
   * @return the pair; aborts with `INVALID_ARGUMENT` when it is not a receipt this service issued
   */
  private def receipt(value: String): IO[StatusException, (String, Long)] =
    ZIO.fromOption(LockReceipt.decode(value)).orElseFail(reject("the receipt is not one this service issued"))

  /**
   * A wire timestamp for a deadline.
   *
   * @param instant the deadline
   * @return the wire timestamp
   */
  private def stamp(instant: Instant): Timestamp = Timestamp(instant.getEpochSecond, instant.getNano)

  /**
   * Refuse a call the caller must change.
   *
   * @param reason what is wrong
   * @return the status
   */
  private def reject(reason: String): StatusException =
    StatusException(Status.INVALID_ARGUMENT.withDescription(reason))

  /**
   * Map a store failure to a status — by the toolkit's markers, as [[QueueService]] does.
   *
   * @param error what went wrong
   * @return the status
   */
  private def status(error: ApplicationError): StatusException = error match
    case _: ApplicationError.TransientError => StatusException(Status.UNAVAILABLE.withDescription(error.message))
    case _                                  => StatusException(Status.INTERNAL.withDescription(""))
