package homelab.keyedqueue.infrastructure.codecs.storage


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.Envelope
import homelab.keyedqueue.domain.service.serialisation.EnvelopeCodec
import homelab.keyedqueue.infrastructure.codecs.grpc.{ Inbound, Outbound }
import homelab.keyedqueue.v1
import zio.{ Chunk, IO, ZIO }


/**
 * Stores an envelope as the same protobuf it arrived as.
 *
 * Reusing the wire message as the storage format is a deliberate economy: a message is written exactly as
 * it was received, so there is one schema to evolve rather than two, and no second mapping to keep honest.
 * The cost is that the stored bytes are now bound to the proto's compatibility rules — field numbers are
 * forever, which they were anyway.
 */
final class ProtobufEnvelopeCodec extends EnvelopeCodec:

  /**
   * Serialise through the same transformer the gRPC layer uses, so a stored message is byte-identical to the
   * one that arrived.
   *
   * @return the bytes to store
   */
  override def encode(envelope: Envelope): Chunk[Byte] =
    Chunk.fromArray(Outbound.toProto(envelope).toByteArray)

  /**
   * Two failures, not one: bytes that are not a protobuf message at all, and a message this version cannot
   * read — an encoding it does not know, say, written by a newer peer. Both are reported as
   * `MalformedReply`, because from a consumer's side the distinction changes nothing.
   *
   * @return the message; aborts when the bytes cannot be read
   */
  override def decode(payload: Chunk[Byte]): IO[QueueError, Envelope] =
    ZIO
      .attempt(v1.Envelope.parseFrom(payload.toArray))
      .mapError(error => QueueError.MalformedReply(s"a stored message is not an envelope: ${error.getMessage}"))
      .flatMap: parsed =>
        ZIO
          .fromEither(Inbound.envelope(parsed))
          .mapError(reason => QueueError.MalformedReply(s"a stored message cannot be read: $reason"))
