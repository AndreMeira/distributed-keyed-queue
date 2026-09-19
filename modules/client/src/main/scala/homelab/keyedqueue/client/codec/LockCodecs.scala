package homelab.keyedqueue.client.codec


import com.google.protobuf.duration.Duration as WireDuration
import com.google.protobuf.timestamp.Timestamp
import homelab.keyedqueue.client.LockError
import homelab.keyedqueue.client.lock.*
import homelab.keyedqueue.v1
import io.grpc.{ Status, StatusException }
import zio.*

import java.time.Instant


/**
 * Wire to Scala and back, for the lock's four calls.
 *
 * Hand-written rather than derived: the wire states a grant as a flag with three fields that mean
 * something only when it is set, and the client states it as a choice. There is no field-for-field
 * correspondence to derive from, and the reading in is partial for that reason — an answer the client
 * cannot hold is refused here rather than carried inwards as options every call site re-examines.
 */
private[client] object LockCodecs:

  /**
   * A duration as the wire carries it.
   *
   * @param duration how long
   * @return its wire form
   */
  def toWire(duration: Duration): WireDuration =
    WireDuration(duration.getSeconds, duration.getNano)

  /**
   * A grant, or the news that somebody else has it.
   *
   * @param response what the service answered
   * @return the answer; fails with `Unreadable` when `acquired` is set without the rest of a grant
   */
  def acquired(response: v1.AcquireResponse): IO[LockError, Acquired] =
    if !response.acquired then ZIO.succeed(Acquired.Unavailable)
    else
      response.leaseExpiresAt match
        case None        => ZIO.fail(LockError.Unreadable("a grant arrived with no lease deadline"))
        case Some(until) =>
          ZIO.succeed(
            Acquired.Granted(Hold(Receipt(response.receipt), Fence(response.fence), instant(until)))
          )

  /**
   * A lease pushed forward, or a hold that is gone.
   *
   * @param response what the service answered
   * @return the answer; fails with `Unreadable` when `renewed` is set without a deadline
   */
  def refreshed(response: v1.RefreshResponse): IO[LockError, Refreshed] =
    if !response.renewed then ZIO.succeed(Refreshed.Lost)
    else
      response.leaseExpiresAt match
        case None        => ZIO.fail(LockError.Unreadable("a renewal arrived with no lease deadline"))
        case Some(until) => ZIO.succeed(Refreshed.Renewed(instant(until)))

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
   * A timestamp as an instant.
   *
   * @param stamp the wire timestamp
   * @return the instant it names
   */
  private def instant(stamp: Timestamp): Instant =
    Instant.ofEpochSecond(stamp.seconds, stamp.nanos.toLong)
