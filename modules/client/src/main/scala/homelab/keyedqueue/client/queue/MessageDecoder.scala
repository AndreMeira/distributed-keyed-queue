package homelab.keyedqueue.client.queue

import zio.Chunk


/**
 * How an arrived message becomes a value.
 *
 * It reads whole messages rather than bytes because it is the only thing that sees the encoding a sender
 * stated, and refusing a message written in a format it does not read is its job — that refusal is what
 * the field is for, since bytes in the wrong format can decode into something wrong rather than fail.
 *
 * @tparam A what it reads
 */
trait MessageDecoder[A]:

  /** @return the media type it reads, such as `application/json` */
  def encoding: String

  /**
   * Read a payload this decoder has already accepted the encoding of.
   *
   * @param payload the bytes as they travelled
   * @return the value, or why the bytes are not one
   */
  protected def read(payload: Chunk[Byte]): Either[String, A]

  /**
   * Read an arrived message, if it is written in the format this reads.
   *
   * @param message what arrived
   * @return the value it carries, or why this decoder cannot produce one
   */
  final def decode(message: Message.Incoming): Either[MessageDecoder.Failure, A] =
    if message.encoding != encoding then Left(MessageDecoder.Failure.WrongEncoding(expected = encoding, found = message.encoding))
    else read(message.payload).left.map(MessageDecoder.Failure.Unreadable.apply)


object MessageDecoder:

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
     * The format was right and the bytes were not a value.
     *
     * @param reason what the reading said was wrong
     */
    case Unreadable(reason: String)
