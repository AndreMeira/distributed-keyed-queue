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

  /** The widest span a proto duration carries, and the longest a lease is read as: ten thousand years. */
  private val maxSpanSeconds: Long = 315576000000L

  /** How many nanoseconds a second holds, which bounds the sub-second field of a timestamp or a span. */
  private val nanosPerSecond: Int = 1000000000

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
   * @return the answer, or `Unreadable` when `acquired` is set without a receipt, a deadline or a lease
   *         span — a grant missing any of the three is one nothing could release, time or renew
   */
  def decode(response: v1.AcquireResponse): Either[LockError, Acquired] =
    if !response.acquired then Right(Acquired.Unavailable)
    else if response.receipt.isEmpty
    then Left(LockError.Unreadable("a grant arrived with no receipt, so nothing could release it"))
    else
      for
        until <- deadline(response.leaseExpiresAt, "a grant")
        ttl   <- span(response.leaseTtl, "a grant")
      yield Acquired.Granted(Hold(Receipt(response.receipt), Fence(response.fence), until, ttl))

  /**
   * A lease pushed forward, or a hold that is gone.
   *
   * @param response what the service answered
   * @return the answer, or `Unreadable` when `renewed` is set without a deadline or a lease span
   */
  def decode(response: v1.RefreshResponse): Either[LockError, Refreshed] =
    if !response.renewed then Right(Refreshed.Lost)
    else
      for
        until <- deadline(response.leaseExpiresAt, "a renewal")
        ttl   <- span(response.leaseTtl, "a renewal")
      yield Refreshed.Renewed(until, ttl)

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
   * When a lease lapses, from the timestamp an answer carried.
   *
   * @param stamp the deadline the answer carried, absent when the service set no field
   * @param answer what the answer was, to say so in the error
   * @return the instant it names, or `Unreadable` when it is absent or names no time
   */
  private def deadline(stamp: Option[Timestamp], answer: String): Either[LockError, Instant] =
    stamp
      .toRight(LockError.Unreadable(s"$answer arrived with no lease deadline"))
      .filterOrElse(namesInstant, LockError.Unreadable(s"$answer arrived with a deadline that names no instant"))
      .map(instant)

  /**
   * How long a lease runs, from the duration an answer carried.
   *
   * @param duration the span the answer carried, absent when the service set no field
   * @param answer what the answer was, to say so in the error
   * @return the span it names, or `Unreadable` when it is absent or is not a length a hold runs for
   */
  private def span(duration: Option[ProtoDuration], answer: String): Either[LockError, Duration] =
    duration
      .toRight(LockError.Unreadable(s"$answer arrived with no lease span"))
      .filterOrElse(namesSpan, LockError.Unreadable(s"$answer arrived with a lease no hold could run for"))
      .map(length)

  /**
   * Whether a timestamp names a time, which is what makes reading it total.
   *
   * @param stamp the wire timestamp
   * @return whether its seconds and nanoseconds are both in range
   */
  private def namesInstant(stamp: Timestamp): Boolean =
    stamp.seconds >= Instant.MIN.getEpochSecond && stamp.seconds <= Instant.MAX.getEpochSecond &&
    stamp.nanos >= 0 && stamp.nanos < nanosPerSecond

  /**
   * Whether a duration names a length a hold runs for: in range, and longer than nothing.
   *
   * @param duration the wire duration
   * @return whether it is a positive span this client reads
   */
  private def namesSpan(duration: ProtoDuration): Boolean =
    duration.seconds >= 0 && duration.seconds <= maxSpanSeconds &&
    duration.nanos >= 0 && duration.nanos < nanosPerSecond &&
    (duration.seconds > 0 || duration.nanos > 0)

  /**
   * A timestamp as an instant, for one that [[namesInstant]] accepted.
   *
   * @param stamp the wire timestamp
   * @return the instant it names
   */
  private def instant(stamp: Timestamp): Instant =
    Instant.ofEpochSecond(stamp.seconds, stamp.nanos.toLong)

  /**
   * A proto duration as a duration, for one that [[namesSpan]] accepted.
   *
   * @param duration the wire duration
   * @return the span it names
   */
  private def length(duration: ProtoDuration): Duration =
    Duration.fromSeconds(duration.seconds) + Duration.fromNanos(duration.nanos.toLong)
