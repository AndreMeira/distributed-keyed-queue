package homelab.keyedqueue.client.queue


import zio.Chunk
import zio.schema.Schema
import zio.schema.codec.{ BinaryCodec, ProtobufCodec }


/**
 * How a value becomes a payload, and what to call the format it was written in.
 *
 * One per type and per format, so the same type may travel as protobuf to one queue and as something else
 * to another. The encoding it names is what an outgoing message states, and what a receiver reads to
 * decide whether it can decode the bytes at all.
 *
 * @tparam A what it writes
 */
trait MessageEncoder[A]:

  /** @return the media type the bytes it writes are in, such as `application/x-protobuf` */
  def encoding: String

  /**
   * Write a value as it will travel.
   *
   * @param value what to write
   * @return its bytes
   */
  def encode(value: A): Chunk[Byte]


object MessageEncoder:

  /** What a schema-derived encoder states its bytes are. */
  val protobuf: String = "application/x-protobuf"

  /**
   * An encoder over a type's schema, writing protobuf.
   *
   * The codec is derived once, here, rather than per message.
   *
   * @tparam A what it writes
   * @return the encoder
   */
  def derive[A: Schema]: MessageEncoder[A] =
    Encoding(ProtobufCodec.protobufCodec[A])

  /**
   * An encoder for anything with a schema, summoned rather than named.
   *
   * Imported with `import MessageEncoder.auto.given`. Building a [[Message.Outgoing]] from a value asks
   * for one of these, so this is what lets a caller pass the value and nothing else.
   */
  object auto:

    /**
     * @tparam A what it writes, which needs a schema in scope
     * @return the encoder over that schema's own format
     */
    given [A: Schema]: MessageEncoder[A] = MessageEncoder.derive

  /**
   * An encoder over a codec that has already been derived.
   *
   * @param codec what turns a value into bytes
   * @tparam A what it writes
   */
  final private class Encoding[A](codec: BinaryCodec[A]) extends MessageEncoder[A]:

    /** @return the media type a schema-derived codec writes */
    override def encoding: String = protobuf

    /**
     * @param value what to write
     * @return its bytes
     */
    override def encode(value: A): Chunk[Byte] = codec.encode(value)
