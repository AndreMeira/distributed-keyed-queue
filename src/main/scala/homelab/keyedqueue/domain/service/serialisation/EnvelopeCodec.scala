package homelab.keyedqueue.domain.service.serialisation


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.Envelope
import zio.{ Chunk, IO }


/**
 * How an envelope becomes the bytes a store holds, and back.
 *
 * A port because the domain has to know that a message is *stored as bytes* — the store is deliberately
 * incurious about cargo — without knowing which format those bytes are in. The adapter picks; today it is
 * the same protobuf the wire uses, which means a message is stored exactly as it arrived.
 *
 * '''Decoding is fallible, encoding is not.''' Anything in the domain can be written; not everything read
 * back was written by a version of this service that agreed with this one.
 */
trait EnvelopeCodec:

  /**
   * Serialise for storage.
   *
   * @param envelope the message
   * @return its bytes
   */
  def encode(envelope: Envelope): Chunk[Byte]

  /**
   * Read back what [[encode]] wrote.
   *
   * @param payload the stored bytes
   * @return the message; aborts with `MalformedReply` when the bytes are not one
   */
  def decode(payload: Chunk[Byte]): IO[QueueError, Envelope]
