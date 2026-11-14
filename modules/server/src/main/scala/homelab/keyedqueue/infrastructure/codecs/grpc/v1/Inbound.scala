package homelab.keyedqueue.infrastructure.codecs.grpc.v1


import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration as ProtoDuration
import com.google.protobuf.timestamp.Timestamp
import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.queue.Message
import homelab.keyedqueue.domain.model.queue.Settlement.Verdict
import homelab.keyedqueue.domain.request.queue.{ DequeueRequest, EnqueueRequest, HeartbeatRequest, ScheduleRequest, SettleRequest }
import homelab.keyedqueue.domain.request.lock.*
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.v1
import io.scalaland.chimney.dsl.*
import io.scalaland.chimney.{ PartialTransformer, Transformer, partial }
import zio.{ Chunk, Duration, duration2DurationOps }

import java.time.{ Instant, Duration as JavaDuration }


/**
 * v1 to domain.
 *
 * Partial: a request that carries no message at all, or no outcome, is refused here, and so is one carrying a
 * `Duration` or a `Timestamp` outside what protobuf allows. Those two are read with a range check rather than
 * converted blindly: an out-of-range one would otherwise read as some other value, or overflow and throw on the
 * request path, where the caller sent something to refuse.
 *
 * `uint32` decodes to a signed `Int`, so a batch size at or above 2^31 reaches the domain negative — which
 * is why `NegativeMaxBatch` is reachable at all.
 */
object Inbound:

  /**
   * A request the domain cannot hold: a `v1.EnqueueRequest` without its `message`, a `v1.SettleRequest` whose
   * `outcomes` hold an unspecified outcome, a duration or a timestamp out of protobuf's range. The caller's to
   * fix, so the protocol layer answers it with `INVALID_ARGUMENT`.
   *
   * Every problem is kept, not only the first, so one round trip tells the caller everything to change.
   *
   * @param reasons each problem, as `path: reason`
   */
  final case class DecodingError(reasons: Chunk[String]) extends ApplicationError.DecodingError:

    /**
     * Every problem, in one line.
     *
     * @return the reasons, separated by `; `
     */
    override def message: String = reasons.mkString("; ")

  /** The longest span, either way, a protobuf `Duration` may carry: about ten thousand years. */
  private val maxDurationSeconds: Long = 315576000000L

  /** The largest sub-second part a protobuf `Duration` or `Timestamp` may carry. */
  private val maxNanos: Int = 999999999

  /** The earliest second a protobuf `Timestamp` may name: 0001-01-01T00:00:00Z. */
  private val minTimestampSeconds: Long = -62135596800L

  /** The latest second a protobuf `Timestamp` may name: 9999-12-31T23:59:59Z. */
  private val maxTimestampSeconds: Long = 253402300799L

  private given Transformer[String, MessageKey] = MessageKey(_)
  private given Transformer[String, MessageId]  = MessageId(_)

  private given Transformer[ByteString, Chunk[Byte]] =
    bytes => Chunk.fromArray(bytes.toByteArray)

  /**
   * A duration in protobuf's range, read; any other refused.
   *
   * Read through `java.time.Duration`, which holds any span protobuf allows, and then into ZIO's, which reads a
   * negative span as zero and one too long for its nanoseconds (about 292 years) as infinite rather than
   * overflowing. Both are well-formed: validation refuses zero and clamps or refuses the infinite, in the domain's
   * terms. Adding the parts as ZIO durations instead overflows past those 292 years.
   */
  private given PartialTransformer[ProtoDuration, Duration] = PartialTransformer: duration =>
    if wellFormed(duration) then partial.Result.fromValue(Duration.fromJava(JavaDuration.ofSeconds(duration.seconds, duration.nanos.toLong)))
    else partial.Result.fromErrorString(s"a duration needs seconds within ±$maxDurationSeconds and nanos within ±$maxNanos, of the same sign")

  /** A timestamp in protobuf's range, read; any other refused. */
  private given PartialTransformer[Timestamp, Instant] = PartialTransformer: stamp =>
    if wellFormed(stamp) then partial.Result.fromValue(Instant.ofEpochSecond(stamp.seconds, stamp.nanos.toLong))
    else partial.Result.fromErrorString(s"a timestamp needs seconds between 0001-01-01 and 9999-12-31 and nanos from 0 to $maxNanos")

  /**
   * An absent duration reads as zero, which validation then refuses: a caller must say how long it will
   * wait rather than leave it to be inferred. Decoded rather than rejected here so the problem is reported
   * with the others in one pass, in the vocabulary of the domain. A present one is range-checked.
   */
  private given PartialTransformer[Option[ProtoDuration], Duration] = PartialTransformer:
    case None        => partial.Result.fromValue(Duration.Zero)
    case Some(proto) => proto.transformIntoPartial[Duration]

  /** A settle that does not say what the consumer decided cannot be acted on. */
  private given PartialTransformer[v1.Outcome, Verdict] = PartialTransformer:
    case v1.Outcome.OUTCOME_DONE   => partial.Result.fromValue(Verdict.Done)
    case v1.Outcome.OUTCOME_FAILED => partial.Result.fromValue(Verdict.Failed)
    case other                     => partial.Result.fromErrorString(s"an outcome is required, got ${other.name}")

  /**
   * An enqueue without a message is a request with nothing in it. Chimney would refuse the absent field on
   * its own, but with a generic reason; this says what the caller left out.
   */
  private given PartialTransformer[Option[v1.Message], EnqueueRequest.Message] = PartialTransformer:
    case Some(message) => message.transformIntoPartial[EnqueueRequest.Message]
    case None          => partial.Result.fromErrorString("a message is required")

  /**
   * Read a message on its own, which is what the storage codec needs.
   *
   * @param message the `v1.Message`
   * @return the domain message, or why it could not be read
   */
  def message(message: v1.Message): Either[String, Message] =
    decoded(message.transformIntoPartial[Message]) match
      case Right(read) => Right(read)
      case Left(error) => Left(error.message)

  /**
   * Whether a protobuf `Duration` is one protobuf allows, which is what makes reading it total.
   *
   * @param duration the protobuf `Duration`
   * @return whether `seconds` and `nanos` are in range, and do not carry opposite signs
   */
  private def wellFormed(duration: ProtoDuration): Boolean =
    duration.seconds >= -maxDurationSeconds && duration.seconds <= maxDurationSeconds &&
    duration.nanos >= -maxNanos && duration.nanos <= maxNanos &&
    !(duration.seconds > 0 && duration.nanos < 0) && !(duration.seconds < 0 && duration.nanos > 0)

  /**
   * Whether a protobuf `Timestamp` is one protobuf allows, which is what makes reading it total.
   *
   * @param stamp the protobuf `Timestamp`
   * @return whether `seconds` falls between 0001-01-01 and 9999-12-31, and `nanos` is not negative nor a second
   */
  private def wellFormed(stamp: Timestamp): Boolean =
    stamp.seconds >= minTimestampSeconds && stamp.seconds <= maxTimestampSeconds &&
    stamp.nanos >= 0 && stamp.nanos <= maxNanos

  /**
   * Turn a partial decode into its value, or into a [[DecodingError]] listing every problem.
   *
   * @param result what the transformer produced
   * @tparam A the domain type
   * @return the value, or every reason it could not be read
   */
  private def decoded[A](result: partial.Result[A]): Either[DecodingError, A] =
    result.asEitherErrorPathMessageStrings match
      case Right(value) => Right(value)
      case Left(errors) => Left(DecodingError(Chunk.fromIterable(errors).map(describe)))

  /**
   * One reason a request could not be read, with the field it concerns.
   *
   * @param error the path to the field, and what was wrong with it
   * @return `path: reason`
   */
  private def describe(error: (String, String)): String =
    s"${error._1}: ${error._2}"

  extension (request: v1.EnqueueRequest)
    /**
     * @return the domain request, or a [[DecodingError]] when the `v1.EnqueueRequest` has no message
     */
    def toDomain: Either[DecodingError, EnqueueRequest] =
      decoded(request.transformIntoPartial[EnqueueRequest])

  extension (request: v1.ScheduleRequest)
    /**
     * @return the domain request, or a [[DecodingError]] when the `v1.ScheduleRequest` has no message; an absent
     *         `deliver_after` reads as zero, which validation refuses with the request's other problems
     */
    def toDomain: Either[DecodingError, ScheduleRequest] =
      decoded(request.transformIntoPartial[ScheduleRequest])

  extension (request: v1.DequeueRequest)
    /** @return the domain request, or a [[DecodingError]] saying why the `v1.DequeueRequest` cannot be read */
    def toDomain: Either[DecodingError, DequeueRequest] =
      decoded(request.transformIntoPartial[DequeueRequest])

  extension (request: v1.SettleRequest)
    /** @return the domain request, or a [[DecodingError]] when an outcome is unspecified */
    def toDomain: Either[DecodingError, SettleRequest] =
      decoded(request.transformIntoPartial[SettleRequest])

  extension (request: v1.HeartbeatRequest)
    /** @return the domain request, or a [[DecodingError]] saying why the `v1.HeartbeatRequest` cannot be read */
    def toDomain: Either[DecodingError, HeartbeatRequest] =
      decoded(request.transformIntoPartial[HeartbeatRequest])

  extension (request: v1.AcquireRequest)
    /**
     * @return the domain request, or a [[DecodingError]] when `ttl` or `max_wait` is out of protobuf's range;
     *         everything else is validated in the use case, not the codec
     */
    def toDomain: Either[DecodingError, AcquireRequest] =
      decoded(request.transformIntoPartial[AcquireRequest])

  extension (request: v1.TryAcquireRequest)
    /** @return the domain request, or a [[DecodingError]] when `ttl` is out of protobuf's range */
    def toDomain: Either[DecodingError, TryAcquireRequest] =
      decoded(request.transformIntoPartial[TryAcquireRequest])

  extension (request: v1.ReleaseRequest)
    /** @return the domain request */
    def toDomain: ReleaseRequest = request.transformInto[ReleaseRequest]

  extension (request: v1.RefreshRequest)
    /** @return the domain request, or a [[DecodingError]] when `ttl` is out of protobuf's range */
    def toDomain: Either[DecodingError, RefreshRequest] =
      decoded(request.transformIntoPartial[RefreshRequest])
