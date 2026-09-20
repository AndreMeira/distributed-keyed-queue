package homelab.keyedqueue.client.queue


import zio.Chunk

import java.time.Instant


/**
 * A message, in whichever direction it is travelling.
 *
 * The payload is bytes here, because that is what the wire carries and it is the one part of a message
 * this client does not know better than the wire. Reading it into a value is [[MessageDecoder]]'s job, and
 * writing one out is [[MessageEncoder]]'s.
 */
sealed trait Message:

  /** @return the key whose order this message takes its place in */
  def key: String

  /** @return what a settle names it by */
  def id: MessageId

  /** @return the schema name and version its payload was written against */
  def payloadType: String

  /** @return the media type its payload is written in */
  def encoding: String

  /** @return the payload, as it travels */
  def payload: Chunk[Byte]


object Message:

  /**
   * One to send.
   *
   * @param key the key whose order it takes its place in; empty is a key of its own
   * @param id what a settle will name it by, and what makes a repeated enqueue idempotent
   * @param payloadType the schema name and version, stated by the caller
   * @param encoding the media type the payload is written in, stated by the encoder that wrote it
   * @param payload the payload, as it travels
   */
  final case class Outgoing(
    key: String,
    id: MessageId,
    payloadType: String,
    encoding: String,
    payload: Chunk[Byte],
  ) extends Message

  object Outgoing:

    /**
     * One to send, carrying a value the encoder writes.
     *
     * The encoder states the encoding as well as the bytes, so the two agree by construction.
     *
     * @param key the key whose order it takes its place in
     * @param id what a settle will name it by
     * @param payloadType the schema name and version, stated by the caller
     * @param value what to send
     * @tparam A what is being sent, which needs an encoder in scope to write it and name its format
     * @return the message to enqueue
     */
    def apply[A: MessageEncoder as encoder](
      key: String,
      id: MessageId,
      payloadType: String,
      value: A,
    ): Outgoing =
      Outgoing(key, id, payloadType, encoder.encoding, encoder.encode(value))

  /**
   * One that arrived, with what only a delivery knows.
   *
   * @param key the key it was ordered within
   * @param id what a settle names it by
   * @param payloadType the schema name and version its sender wrote it against
   * @param encoding the media type its payload is written in
   * @param payload the payload, as it travelled
   * @param sentAt when its sender says it sent it, on the sender's clock
   * @param attempt how many times it has been delivered; 1 on the first
   */
  final case class Incoming(
    key: String,
    id: MessageId,
    payloadType: String,
    encoding: String,
    payload: Chunk[Byte],
    sentAt: Instant,
    attempt: Int,
  ) extends Message
