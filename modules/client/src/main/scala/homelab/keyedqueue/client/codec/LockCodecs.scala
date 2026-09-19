package homelab.keyedqueue.client.codec


import com.google.protobuf.duration.Duration as ProtoDuration
import com.google.protobuf.timestamp.Timestamp
import homelab.keyedqueue.client.LockError
import homelab.keyedqueue.client.lock.*
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
   * A duration as the proto carries it.
   *
   * @param duration how long
   * @return its proto form
   */
  def encode(duration: Duration): ProtoDuration =
    ProtoDuration(duration.getSeconds, duration.getNano)

  /**
   * A grant, or the news that somebody else has it.
   *
   * @param response what the service answered
   * @return the answer, or `Unreadable` when `acquired` is set without a receipt or a deadline — a grant
   *         missing either is one nothing could release or renew
   */
  def decode(response: v1.AcquireResponse): Either[LockError, Acquired] =
    if !response.acquired then Right(Acquired.Unavailable)
    else if response.receipt.isEmpty then Left(LockError.Unreadable("a grant arrived with no receipt, so nothing could release it"))
    else
      response.leaseExpiresAt
        .toRight(LockError.Unreadable("a grant arrived with no lease deadline"))
        .map(instant)
        .map(granted(response))

  /**
   * A lease pushed forward, or a hold that is gone.
   *
   * @param response what the service answered
   * @return the answer, or `Unreadable` when `renewed` is set without a deadline
   */
  def decode(response: v1.RefreshResponse): Either[LockError, Refreshed] =
    if !response.renewed then Right(Refreshed.Lost)
    else
      response.leaseExpiresAt
        .toRight(LockError.Unreadable("a renewal arrived with no lease deadline"))
        .map(instant)
        .map(Refreshed.Renewed.apply)

  /**
   * What a transport failure means to a caller.
   *
   * @param failure what the call raised
   * @return the error to report
   */
  def failure(failure: StatusException): LockError =
    failure.getStatus.getCode match
      case Status.Code.INVALID_ARGUMENT  => LockError.Rejected(Option(failure.getStatus.getDescription).getOrElse(""))
      case Status.Code.UNAVAILABLE       => LockError.Unreachable(failure)
      case Status.Code.DEADLINE_EXCEEDED => LockError.Unreachable(failure)
      case _                             => LockError.Failed(failure)

  /**
   * The grant a response describes, once its deadline has been read.
   *
   * @param response what the service answered
   * @param until when the lease lapses
   * @return the grant
   */
  private def granted(response: v1.AcquireResponse)(until: Instant): Acquired =
    Acquired.Granted(Hold(Receipt(response.receipt), Fence(response.fence), until))

  /**
   * A timestamp as an instant.
   *
   * @param stamp the wire timestamp
   * @return the instant it names
   */
  private def instant(stamp: Timestamp): Instant =
    Instant.ofEpochSecond(stamp.seconds, stamp.nanos.toLong)
