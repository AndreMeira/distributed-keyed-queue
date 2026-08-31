package homelab.keyedqueue.infrastructure.codecs.grpc.v1


import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration as WireDuration
import com.google.protobuf.timestamp.Timestamp
import homelab.keyedqueue.domain.model.Message
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
 * Every transformer here is derived. That is the test: the domain requests mirror their wire messages field
 * for field, so anything Chimney cannot work out on its own is a place the two have drifted, not a place
 * for a hand-written mapping to paper over.
 *
 * '''Partial, because the wire can say things the domain cannot hold.''' A proto3 enum always carries an
 * `UNSPECIFIED`, and a message field is always optional, so a request that says nothing about its encoding
 * or carries no message at all is representable on the wire and meaningless here. Those are refused at
 * this boundary rather than becoming states every later match has to remember to reject.
 */
object Inbound:

  /**
   * The only domain names minted here, and not on any request's behalf.
   *
   * Both exist for [[message]], which the storage codec uses to read back what this service itself wrote.
   * Those bytes came from a message that had already been parsed, so reconstructing its names restores
   * evidence rather than inventing it.
   *
   * '''No request crosses through them.''' Every request's fields arrive as the strings they are, and the
   * names are minted by the parse, once something has checked them — which is why there is no
   * `String => QueueName` or `String => ClaimRef` here to reach for.
   */
  private given Transformer[String, MessageKey] = MessageKey(_)
  private given Transformer[String, MessageId]  = MessageId(_)

  private given Transformer[ByteString, Chunk[Byte]] =
    bytes => Chunk.fromArray(bytes.toByteArray)

  private given Transformer[WireDuration, Duration] =
    duration => Duration.fromSeconds(duration.seconds) + Duration.fromNanos(duration.nanos.toLong)

  private given Transformer[Timestamp, Instant] = stamp => Instant.ofEpochSecond(stamp.seconds, stamp.nanos.toLong)

  /** An absent duration means "do not wait", which is a legitimate request rather than a missing field. */
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
   *
   * Targets the request's own [[QueueRequest.Enqueue.Message]], not the domain's: the domain one carries a
   * key and an id that have been checked, and this boundary checks nothing — it reads what arrived.
   */
  private given PartialTransformer[Option[v1.Message], QueueRequest.Enqueue.Message] = PartialTransformer:
    case Some(message) => message.transformIntoPartial[QueueRequest.Enqueue.Message]
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
    def toDomain: partial.Result[QueueRequest.Enqueue] = request.transformIntoPartial[QueueRequest.Enqueue]

  extension (request: v1.DequeueRequest)
    /** @return the domain request */
    def toDomain: partial.Result[QueueRequest.Dequeue] = request.transformIntoPartial[QueueRequest.Dequeue]

  extension (request: v1.SettleRequest)
    /** @return the domain request; fails when the outcome is unspecified */
    def toDomain: partial.Result[QueueRequest.Settle] = request.transformIntoPartial[QueueRequest.Settle]

  extension (request: v1.HeartbeatRequest)
    /** @return the domain request */
    def toDomain: partial.Result[QueueRequest.Heartbeat] = request.transformIntoPartial[QueueRequest.Heartbeat]
