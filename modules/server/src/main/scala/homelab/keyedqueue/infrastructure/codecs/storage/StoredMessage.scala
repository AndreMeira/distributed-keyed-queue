package homelab.keyedqueue.infrastructure.codecs.storage


import homelab.keyedqueue.domain.model.Message
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.codecs.grpc.v1.{ Inbound, Outbound }
import homelab.keyedqueue.v1
import zio.Chunk

import scala.util.Try


/**
 * How a message is written into a store, and read back.
 *
 * '''Not a port, deliberately.''' Serialisation is the storing adapter's business in the same way key layout
 * is: `QueueStore` speaks in messages, and whoever implements it decides what a message looks like at rest.
 * A second backend is free to store rows instead, and nothing above it changes.
 *
 * Reusing the wire message as the storage format is an economy: a message is written exactly as it was
 * received, so there is one schema to evolve rather than two, and no second mapping to keep honest. The cost
 * is that stored bytes now live by the proto's compatibility rules — field numbers are forever, which they
 * were anyway.
 */
object StoredMessage:

  /**
   * Serialise through the same transformer the gRPC layer uses, so a stored message is byte-identical to the
   * one that arrived.
   *
   * @param message the message
   * @return the bytes to store
   */
  def toBytes(message: Message): Chunk[Byte] =
    Chunk.fromArray(Outbound.toProto(message).toByteArray)

  /**
   * Read back what [[toBytes]] wrote.
   *
   * Two failures, not one: bytes that are not a protobuf message at all, and a message this version cannot
   * read — an encoding it does not know, say, written by a newer peer. Both are reported as `MalformedReply`,
   * because the store is the party that produced them and from a consumer's side the distinction changes
   * nothing.
   *
   * @param bytes what the store handed back
   * @return the message, or `MalformedReply` when the bytes cannot be read
   */
  def fromBytes(bytes: Chunk[Byte]): Either[RedisFailure, Message] =
    Try(v1.Message.parseFrom(bytes.toArray)).toEither.left
      .map(error => RedisFailure.MalformedReply(s"a stored message is not a message: ${error.getMessage}"))
      .flatMap: parsed =>
        Inbound
          .message(parsed)
          .left
          .map(reason => RedisFailure.MalformedReply(s"a stored message cannot be read: $reason"))
