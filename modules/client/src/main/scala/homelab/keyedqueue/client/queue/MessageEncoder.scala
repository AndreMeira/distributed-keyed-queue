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

  /** @return the schema name and version the messages it writes state, or [[MessageEncoder.unnamed]] */
  def payloadType: String

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

  /**
   * The encoder a caller has in scope for a type.
   *
   * Saves naming the instance where one is already there to be found: `MessageEncoder[Order]` rather than
   * the value it was bound to.
   *
   * @tparam A what it writes, which needs an encoder in scope
   * @return that encoder
   */
  def apply[A: MessageEncoder as encoder]: MessageEncoder[A] = encoder

  /** What a schema-derived encoder states its bytes are. */
  val protobuf: String = "application/x-protobuf"

  /**
   * What a message states when nobody has named what it carries.
   *
   * The service never reads the field, so this costs nothing on the wire — but a consumer reading it will
   * learn only that the sender did not say. A decoder built to expect this literal accepts exactly these
   * messages and no others, which is what makes the pair agree without either side matching wildcards.
   */
  val unnamed: String = "*"

  /**
   * An encoder over a type's schema, writing protobuf, naming nothing.
   *
   * What it writes states [[unnamed]] as its payload type, so a consumer learns the format and not the
   * contract. [[deriveAs]] is the same encoder with a name.
   *
   * @tparam A what it writes
   * @return the encoder
   */
  def derive[A: Schema]: MessageEncoder[A] =
    deriveAs(unnamed)

  /**
   * An encoder over a type's schema, writing protobuf under a name.
   *
   * The name is stated here rather than derived because a schema knows its structure and not what an
   * organisation agreed to call it, and because the same type may travel under different names.
   *
   * @param payloadType the schema name and version its messages state
   * @tparam A what it writes
   * @return the encoder
   */
  def deriveAs[A: Schema](payloadType: String): MessageEncoder[A] =
    Encoding(payloadType, ProtobufCodec.protobufCodec[A])

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
   * @param payloadType the schema name and version its messages state
   * @param codec what turns a value into bytes
   * @tparam A what it writes
   */
  final private class Encoding[A](override val payloadType: String, codec: BinaryCodec[A]) extends MessageEncoder[A]:

    /** @return the media type a schema-derived codec writes */
    override def encoding: String = protobuf

    /**
     * @param value what to write
     * @return its bytes
     */
    override def encode(value: A): Chunk[Byte] = codec.encode(value)
