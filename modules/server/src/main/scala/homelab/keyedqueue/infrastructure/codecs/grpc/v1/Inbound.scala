package homelab.keyedqueue.infrastructure.codecs.grpc.v1


import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration as ProtoDuration
import com.google.protobuf.timestamp.Timestamp
import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.queue.Message
import homelab.keyedqueue.domain.model.queue.Settlement.Verdict
import homelab.keyedqueue.domain.request.queue.{ DequeueRequest, EnqueueRequest, HeartbeatRequest, SettleRequest }
import homelab.keyedqueue.domain.request.lock.*
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.v1
import io.scalaland.chimney.dsl.*
import io.scalaland.chimney.{ PartialTransformer, Transformer, partial }
import zio.{ Chunk, Duration, duration2DurationOps }

import java.time.Instant


/**
 * v1 to domain.
 *
 * Partial: a request that carries no message at all, or no outcome, is refused here.
 *
 * `uint32` decodes to a signed `Int`, so a batch size at or above 2^31 reaches the domain negative — which
 * is why `NegativeMaxBatch` is reachable at all.
 */
object Inbound:

  /**
   * A request the domain cannot hold: a `v1.EnqueueRequest` without its `message`, a `v1.SettleRequest` whose
   * `outcomes` hold an unspecified outcome. The caller's to fix, so the protocol layer answers it with
   * `INVALID_ARGUMENT`.
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

  private given Transformer[String, MessageKey] = MessageKey(_)
  private given Transformer[String, MessageId]  = MessageId(_)

  private given Transformer[ByteString, Chunk[Byte]] =
    bytes => Chunk.fromArray(bytes.toByteArray)

  private given Transformer[ProtoDuration, Duration] =
    duration => Duration.fromSeconds(duration.seconds) + Duration.fromNanos(duration.nanos.toLong)

  private given Transformer[Timestamp, Instant] =
    stamp => Instant.ofEpochSecond(stamp.seconds, stamp.nanos.toLong)

  /**
   * An absent duration reads as zero, which validation then refuses: a caller must say how long it will
   * wait rather than leave it to be inferred. Decoded rather than rejected here so the problem is reported
   * with the others in one pass, in the vocabulary of the domain.
   */
  private given Transformer[Option[ProtoDuration], Duration] = {
    case None        => Duration.Zero
    case Some(proto) => proto.transformInto[Duration]
  }

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
    def toDomain: Either[DecodingError, EnqueueRequest] = decoded(request.transformIntoPartial[EnqueueRequest])

  extension (request: v1.DequeueRequest)
    /** @return the domain request, or a [[DecodingError]] saying why the `v1.DequeueRequest` cannot be read */
    def toDomain: Either[DecodingError, DequeueRequest] = decoded(request.transformIntoPartial[DequeueRequest])

  extension (request: v1.SettleRequest)
    /** @return the domain request, or a [[DecodingError]] when an outcome is unspecified */
    def toDomain: Either[DecodingError, SettleRequest] = decoded(request.transformIntoPartial[SettleRequest])

  extension (request: v1.HeartbeatRequest)
    /** @return the domain request, or a [[DecodingError]] saying why the `v1.HeartbeatRequest` cannot be read */
    def toDomain: Either[DecodingError, HeartbeatRequest] = decoded(request.transformIntoPartial[HeartbeatRequest])

  extension (request: v1.AcquireRequest)
    /** @return the domain request; total, because validation happens in the use case, not the codec */
    def toDomain: AcquireRequest = request.transformInto[AcquireRequest]

  extension (request: v1.TryAcquireRequest)
    /** @return the domain request */
    def toDomain: TryAcquireRequest = request.transformInto[TryAcquireRequest]

  extension (request: v1.ReleaseRequest)
    /** @return the domain request */
    def toDomain: ReleaseRequest = request.transformInto[ReleaseRequest]

  extension (request: v1.RefreshRequest)
    /** @return the domain request */
    def toDomain: RefreshRequest = request.transformInto[RefreshRequest]
