package homelab.keyedqueue.infrastructure.codecs.grpc.v1


import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration as WireDuration
import com.google.protobuf.timestamp.Timestamp
import homelab.keyedqueue.domain.model.Message
import homelab.keyedqueue.domain.model.Message.Encoding
import homelab.keyedqueue.domain.model.Settlement.Verdict
import homelab.keyedqueue.domain.request.v1.*
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.v1
import io.scalaland.chimney.dsl.*
import io.scalaland.chimney.{ partial, PartialTransformer, Transformer }
import zio.{ duration2DurationOps, Chunk, Duration }

import java.time.Instant


/**
 * Wire to domain.
 *
 * Partial: a request that names no encoding, or carries no message at all, is refused here.
 *
 * `uint32` decodes to a signed `Int`, so a batch size at or above 2^31 reaches the domain negative — which
 * is why `NegativeMaxBatch` is reachable at all.
 */
object Inbound:

  private given Transformer[String, MessageKey] = MessageKey(_)
  private given Transformer[String, MessageId]  = MessageId(_)

  private given Transformer[ByteString, Chunk[Byte]] =
    bytes => Chunk.fromArray(bytes.toByteArray)

  private given Transformer[WireDuration, Duration] =
    duration => Duration.fromSeconds(duration.seconds) + Duration.fromNanos(duration.nanos.toLong)

  private given Transformer[Timestamp, Instant] = stamp => Instant.ofEpochSecond(stamp.seconds, stamp.nanos.toLong)

  /**
   * An absent duration reads as zero, which validation then refuses: a caller must say how long it will
   * wait rather than leave it to be inferred. Decoded rather than rejected here so the problem is reported
   * with the others in one pass, in the vocabulary of the domain.
   */
  private given Transformer[Option[WireDuration], Duration] =
    _.fold(Duration.Zero)(duration => Duration.fromSeconds(duration.seconds) + Duration.fromNanos(duration.nanos.toLong))

  /** A message that does not say how to read it cannot be acted on. */
  private given PartialTransformer[v1.Encoding, Encoding] = PartialTransformer:
    case v1.Encoding.ENCODING_JSON     => partial.Result.fromValue(Encoding.Json)
    case v1.Encoding.ENCODING_PROTOBUF => partial.Result.fromValue(Encoding.Protobuf)
    case other                         => partial.Result.fromErrorString(s"unsupported encoding: ${other.name}")

  /** Nor can one that does not say what the consumer decided. */
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
   * @param message the wire message
   * @return the domain message, or why it could not be read
   */
  def message(message: v1.Message): Either[String, Message] =
    message
      .transformIntoPartial[Message]
      .asEitherErrorPathMessageStrings
      .left
      .map(_.map((path, reason) => s"$path: $reason").mkString("; "))

  extension (request: v1.EnqueueRequest)
    /**
     * @return the domain request; fails with the reasons the wire message could not be read
     */
    def toDomain: partial.Result[EnqueueRequest] = request.transformIntoPartial[EnqueueRequest]

  extension (request: v1.DequeueRequest)
    /** @return the domain request */
    def toDomain: partial.Result[DequeueRequest] = request.transformIntoPartial[DequeueRequest]

  extension (request: v1.SettleRequest)
    /** @return the domain request; fails when the outcome is unspecified */
    def toDomain: partial.Result[SettleRequest] = request.transformIntoPartial[SettleRequest]

  extension (request: v1.HeartbeatRequest)
    /** @return the domain request */
    def toDomain: partial.Result[HeartbeatRequest] = request.transformIntoPartial[HeartbeatRequest]

  extension (request: v1.AcquireRequest)
    /** @return the domain request; total, because validation happens in the use case, not the codec */
    def toDomain: AcquireRequest = request.transformInto[AcquireRequest]

  extension (request: v1.ReleaseRequest)
    /** @return the domain request */
    def toDomain: ReleaseRequest = request.transformInto[ReleaseRequest]

  extension (request: v1.RefreshRequest)
    /** @return the domain request */
    def toDomain: RefreshRequest = request.transformInto[RefreshRequest]
