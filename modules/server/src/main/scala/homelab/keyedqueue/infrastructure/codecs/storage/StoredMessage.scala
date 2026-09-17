package homelab.keyedqueue.infrastructure.codecs.storage


import homelab.keyedqueue.domain.model.queue.Message
import homelab.keyedqueue.infrastructure.codecs.grpc.v1.{ Inbound, Outbound }
import homelab.keyedqueue.v1
import zio.Chunk

import scala.util.Try


/**
 * How a message is written into a store, and read back.
 *
 * Serialisation is the storing adapter's business in the same way key layout is: `QueueStore` speaks in
 * messages, and whoever implements it decides what a message looks like at rest. A message is written
 * exactly as it arrived, so there is one schema to evolve rather than two — at the cost that stored bytes
 * live by the proto's compatibility rules.
 */
object StoredMessage:

  /**
   * Why a stored message could not be read back.
   *
   * Belongs to the decoder, not to any one store: a backend maps this to whatever it reports failures with.
   */
  enum Failure:

    /** The bytes are not a protobuf message at all. */
    case NotAMessage(reason: String)

    /** A message this version cannot read — an encoding it does not know, written by a newer peer. */
    case Unreadable(reason: String)

    /**
     * What went wrong, phrased for whoever reports it.
     *
     * @return the description
     */
    def message: String = this match
      case NotAMessage(reason) => s"a stored message is not a message: $reason"
      case Unreadable(reason)  => s"a stored message cannot be read: $reason"

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
   * @param bytes what the store handed back
   * @return the message, or why it could not be read
   */
  def fromBytes(bytes: Chunk[Byte]): Either[Failure, Message] =
    Try(v1.Message.parseFrom(bytes.toArray)).toEither.left
      .map(error => Failure.NotAMessage(error.getMessage))
      .flatMap: parsed =>
        Inbound.message(parsed).left.map(Failure.Unreadable(_))
