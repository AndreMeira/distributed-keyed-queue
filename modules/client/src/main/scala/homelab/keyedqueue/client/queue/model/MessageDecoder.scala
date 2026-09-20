package homelab.keyedqueue.client.queue.model


import zio.schema.Schema
import zio.schema.codec.{ BinaryCodec, DecodeError, ProtobufCodec }


/**
 * How an arrived message becomes a value.
 *
 * It reads whole messages rather than bytes because it is the only thing that sees what a sender said it
 * wrote — the encoding and the payload type. Whether to hold a message to those is the caller's choice:
 * [[MessageDecoder.derive]] reads whatever arrives, MessageDecoder.derive refuses anything a
 * sender labelled as something else.
 *
 * @tparam A what it reads
 */
trait MessageDecoder[A]:

  /**
   * Read an arrived message.
   *
   * @param message what arrived
   * @return the value it carries, or why this decoder cannot produce one
   */
  def decode(message: Message.Incoming): Either[MessageDecoder.Failure, A]


object MessageDecoder:

  /**
   * The decoder a caller has in scope for a type.
   *
   * Saves naming the instance where one is already there to be found: `MessageDecoder[Order]` rather than
   * the value it was bound to.
   *
   * @tparam A what it reads, which needs a decoder in scope
   * @return that decoder
   */
  def apply[A: MessageDecoder as decoder]: MessageDecoder[A] = decoder

  /**
   * A decoder for one payload type, over that type's schema.
   *
   * What a consumer of a single kind of message wants: it reads the schema's own format and refuses
   * anything a sender labelled as something else. The no-argument `derive` is the same without the
   * refusal.
   *
   * The encoding is the schema codec's own rather than an argument, so the format this accepts and the
   * format it reads are the same statement. Pass [[MessageEncoder.unnamed]] to accept exactly what an
   * encoder that names nothing writes.
   *
   * @param payloadType the schema name and version to accept
   * @tparam A what it reads
   * @return the decoder
   */
  def deriveAs[A: Schema](payloadType: String): MessageDecoder[A] =
    MessageDecoder.Verifying(MessageEncoder.protobuf, payloadType, ProtobufCodec.protobufCodec[A])

  /**
   * The same, over a codec this client did not derive.
   *
   * The encoding is stated here because only the caller knows what its codec writes; stating one the codec
   * does not write is what this refusal exists to catch elsewhere.
   *
   * @param payloadType the schema name and version to accept
   * @param encoding the media type the codec reads, which is what this accepts
   * @tparam A what it reads
   * @return the decoder
   */
  def deriveAs[A: BinaryCodec as codec](payloadType: String, encoding: String): MessageDecoder[A] =
    MessageDecoder.Verifying(encoding, payloadType, codec)

  /**
   * A decoder over a type's schema, reading protobuf.
   *
   * Reads the bytes and says nothing about what the sender claimed they are; [[deriveAs]] is the same
   * reading with that refusal in front of it.
   *
   * @tparam A what it reads
   * @return the decoder
   */
  def derive[A: Schema]: MessageDecoder[A] =
    MessageDecoder.Trusting(ProtobufCodec.protobufCodec[A])

  /**
   * What a failure to read amounts to, as a value the reading answers with.
   *
   * @param message what arrived, which is what says the format the bytes were supposed to be in
   * @param error what the codec said was wrong
   * @return the failure
   */
  private def unreadable(message: Message.Incoming)(error: DecodeError): Failure =
    Failure.Unreadable(error.message, message.encoding, message.payloadType)

  /**
   * A decoder that takes the sender's word for what the bytes are.
   *
   * @param codec what turns bytes into a value
   * @tparam A what it reads
   */
  final private class Trusting[A](codec: BinaryCodec[A]) extends MessageDecoder[A]:

    /**
     * @param message what arrived
     * @return the value its payload carries, or why the bytes are not one
     */
    override def decode(message: Message.Incoming): Either[Failure, A] =
      codec.decode(message.payload).left.map(unreadable(message))

  /**
   * A decoder that checks what a sender claimed before reading anything.
   *
   * @param encoding the media type to accept
   * @param payloadType the schema name and version to accept
   * @param codec what reads the bytes once they are accepted
   * @tparam A what it reads
   */
  final private class Verifying[A](encoding: String, payloadType: String, codec: BinaryCodec[A]) extends MessageDecoder[A]:

    /**
     * @param message what arrived
     * @return the value it carries, or why this decoder will not read it
     */
    override def decode(message: Message.Incoming): Either[Failure, A] =
      if message.encoding != encoding then Left(Failure.WrongEncoding(encoding, message.encoding))
      else if message.payloadType != payloadType then Left(Failure.WrongType(payloadType, message.payloadType))
      else codec.decode(message.payload).left.map(unreadable(message))

  /**
   * Why a message did not become a value.
   *
   * A settle is still the caller's to send either way: the message and its id are in hand before any of
   * this is attempted.
   */
  enum Failure:

    /**
     * The sender wrote a format this decoder does not read.
     *
     * @param expected what this decoder reads
     * @param found what the message says it is
     */
    case WrongEncoding(expected: String, found: String)

    /**
     * The sender wrote something this decoder is not for.
     *
     * @param expected what this decoder reads
     * @param found what the message says it is
     */
    case WrongType(expected: String, found: String)

    /**
     * The bytes were not a value.
     *
     * What the sender said they were comes with it, because a decoder that checks neither most often fails
     * here for exactly that reason, and the reason alone does not say which it was.
     *
     * @param reason what the reading said was wrong
     * @param encoding the media type the message said its payload was in
     * @param payloadType the schema name and version the message said it was written against
     */
    case Unreadable(reason: String, encoding: String, payloadType: String)

  /**
   * A decoder for anything with a schema, summoned rather than named.
   *
   * Imported with `import MessageDecoder.auto.given`, so it is a caller's choice rather than something in
   * scope by default. What it hands over is [[derive]]: it reads what arrives and says nothing about what
   * the sender called it, so a consumer that wants a message refused for being something else names
   * [[deriveAs]] instead.
   */
  object auto:

    /**
     * @tparam A what it reads, which needs a schema in scope
     * @return the decoder over that schema's own format
     */
    given [A: Schema]: MessageDecoder[A] = MessageDecoder.derive
