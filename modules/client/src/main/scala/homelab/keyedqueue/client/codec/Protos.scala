package homelab.keyedqueue.client.codec


import com.google.protobuf.duration.Duration as ProtoDuration
import com.google.protobuf.timestamp.Timestamp
import homelab.keyedqueue.client.ServiceError
import io.grpc.{ Status, StatusException }
import zio.*

import java.time.Instant


/**
 * Reading the times and spans the wire carries, for both halves of the service.
 *
 * Total, which is the only interesting thing about it: nothing on the wire bounds these fields, so a
 * malformed answer would otherwise reach a caller as a thrown exception rather than as the refusal every
 * other unreadable answer becomes.
 */
private[client] object Protos:

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
   * When something lapses, from the timestamp an answer carried.
   *
   * @param stamp the deadline the answer carried, absent when the service set no field
   * @param answer what the answer was, to say so in the error
   * @return the instant it names, or `Unreadable` when it is absent or names no time
   */
  def deadline(stamp: Option[Timestamp], answer: String): Either[ServiceError, Instant] =
    stamp
      .toRight(ServiceError.Unreadable(s"$answer arrived with no deadline"))
      .filterOrElse(namesInstant, ServiceError.Unreadable(s"$answer arrived with a deadline that names no instant"))
      .map(instant)

  /**
   * How long a lease runs, from the duration an answer carried.
   *
   * @param duration the span the answer carried, absent when the service set no field
   * @param answer what the answer was, to say so in the error
   * @return the span it names, or `Unreadable` when it is absent or is not a length a lease runs for
   */
  def span(duration: Option[ProtoDuration], answer: String): Either[ServiceError, Duration] =
    duration
      .toRight(ServiceError.Unreadable(s"$answer arrived with no lease span"))
      .filterOrElse(namesSpan, ServiceError.Unreadable(s"$answer arrived with a lease no hold could run for"))
      .map(length)

  /**
   * What a transport failure means to a caller.
   *
   * @param failure what the call raised
   * @return the error to report
   */
  def failure(failure: StatusException): ServiceError =
    failure.getStatus.getCode match
      case Status.Code.INVALID_ARGUMENT  => ServiceError.Rejected(Option(failure.getStatus.getDescription).getOrElse(""))
      case Status.Code.UNAVAILABLE       => ServiceError.Unreachable(failure)
      case Status.Code.DEADLINE_EXCEEDED => ServiceError.Unreachable(failure)
      case _                             => ServiceError.Failed(failure)

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
   * Whether a duration names a length a lease runs for: in range, and longer than nothing.
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
