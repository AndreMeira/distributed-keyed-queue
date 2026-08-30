package homelab.keyedqueue.infrastructure.codecs.grpc.v1


import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import homelab.keyedqueue.domain.model.{ Delivery, Message }
import homelab.keyedqueue.domain.response.v1.*
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.v1
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.*
import zio.Chunk

import java.time.Instant


/**
 * Domain to v1.
 *
 * Total, unlike [[Inbound]]: everything the domain can hold has a wire representation, because the wire is
 * strictly the wider of the two — it also has the `UNSPECIFIED` cases the domain refuses to carry. That
 * asymmetry is the point, and it is why only one direction needs to be able to fail.
 */
object Outbound:

  /**
   * Every ScalaPB message carries an `unknownFields` the domain has no counterpart for, and every one has a
   * default. Enabling defaults once here is what keeps these transformers derivable rather than hand-written
   * — the alternative is a `define`/`buildTransformer` incantation per message, which would bury the one
   * thing worth noticing: that nothing else needs mapping.
   */
  transparent inline private given TransformerConfiguration[?] =
    TransformerConfiguration.default.enableDefaultValues

  private given Transformer[QueueName, String]  = identity(_)
  private given Transformer[MessageKey, String] = identity(_)
  private given Transformer[MessageId, String]  = identity(_)

  // proto3 has no presence for a scalar, so absence is the empty string — which is what "no claim" looks
  // like on the wire, and why `deliveries` being empty says the same thing.
  private given Transformer[Option[ClaimRef], String] = _.getOrElse(ClaimRef(""))

  // Likewise for the lease: no claim, no deadline.
  private given Transformer[Option[Instant], Option[Timestamp]] =
    _.map(instant => Timestamp(instant.getEpochSecond, instant.getNano))
  private given Transformer[ClaimRef, String]                   = identity(_)

  private given Transformer[Chunk[Byte], ByteString] =
    bytes => ByteString.copyFrom(bytes.toArray)

  private given Transformer[Instant, Timestamp] =
    instant => Timestamp(seconds = instant.getEpochSecond, nanos = instant.getNano)

  private given Transformer[Instant, Option[Timestamp]] =
    instant => Some(Timestamp(seconds = instant.getEpochSecond, nanos = instant.getNano))

  private given Transformer[Encoding, v1.Encoding] =
    case Encoding.Json     => v1.Encoding.ENCODING_JSON
    case Encoding.Protobuf => v1.Encoding.ENCODING_PROTOBUF

  private given Transformer[Applied, v1.Applied] =
    case Applied.Ok    => v1.Applied.APPLIED_OK
    case Applied.Stale => v1.Applied.APPLIED_STALE

  private given Transformer[Message, Option[v1.Message]] =
    message => Some(toProto(message))

  private given Transformer[Delivery, v1.Delivery] = Transformer.derive[Delivery, v1.Delivery]

  /**
   * The wire form of a message, which is also how it is stored.
   *
   * @param message the message
   * @return its wire form
   */
  def toProto(message: Message): v1.Message =
    message.transformInto[v1.Message]

  extension (response: EnqueueResponse)
    /** @return the wire response */
    def toProto: v1.EnqueueResponse =
      response.transformInto[v1.EnqueueResponse]

  extension (response: DequeueResponse)
    /** @return the wire response; an absent delivery is a timeout, not an error */
    def toProto: v1.DequeueResponse = response.transformInto[v1.DequeueResponse]

  extension (response: SettleResponse)
    /** @return the wire response */
    def toProto: v1.SettleResponse = response.transformInto[v1.SettleResponse]

  extension (response: HeartbeatResponse)
    /** @return the wire response */
    def toProto: v1.HeartbeatResponse = response.transformInto[v1.HeartbeatResponse]
