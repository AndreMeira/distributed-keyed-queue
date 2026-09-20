package homelab.keyedqueue.client.codec


import com.google.protobuf.duration.Duration as ProtoDuration
import com.google.protobuf.timestamp.Timestamp
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.lock.*
import homelab.keyedqueue.client.lock.model.{ Acquired, Fence, Hold, Receipt, Refreshed }
import homelab.keyedqueue.v1
import io.grpc.{ Status, StatusException }
import zio.*

import java.time.Instant


/**
 * Proto to Scala and back, for the lock's four calls.
 *
 * Pure: these are decisions about what arrived, so a caller lifts them where it needs an effect. Hand-written
 * rather than derived, because the proto states a grant as a flag with three fields that mean
 * something only when it is set, and the client states it as a choice. There is no field-for-field
 * correspondence to derive from, and the reading in is partial for that reason — an answer the client
 * cannot hold is refused here rather than carried inwards as options every call site re-examines.
 */
private[client] object LockCodecs:

  /**
   * A grant, or the news that somebody else has it.
   *
   * @param response what the service answered
   * @return the answer, or `Unreadable` when `acquired` is set without a receipt, a deadline or a lease
   *         span — a grant missing any of the three is one nothing could release, time or renew
   */
  def decode(response: v1.AcquireResponse): Either[ServiceError, Acquired] =
    if !response.acquired then Right(Acquired.Unavailable)
    else if response.receipt.isEmpty
    then Left(ServiceError.Unreadable("a grant arrived with no receipt, so nothing could release it"))
    else
      for
        until <- Protos.deadline(response.leaseExpiresAt, "a grant")
        ttl   <- Protos.span(response.leaseTtl, "a grant")
      yield Acquired.Granted(Hold(Receipt(response.receipt), Fence(response.fence), until, ttl))

  /**
   * A lease pushed forward, or a hold that is gone.
   *
   * @param response what the service answered
   * @return the answer, or `Unreadable` when `renewed` is set without a deadline or a lease span
   */
  def decode(response: v1.RefreshResponse): Either[ServiceError, Refreshed] =
    if !response.renewed then Right(Refreshed.Lost)
    else
      for
        until <- Protos.deadline(response.leaseExpiresAt, "a renewal")
        ttl   <- Protos.span(response.leaseTtl, "a renewal")
      yield Refreshed.Renewed(until, ttl)
