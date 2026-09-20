package homelab.keyedqueue.client.queue

import zio.Chunk


/**
 * How a value becomes a payload, and what to call the format it was written in.
 *
 * One per type and per format, so the same type may travel as JSON to one queue and as protobuf to
 * another. The encoding it names is what an outgoing message states, which is what a receiver reads to
 * decide whether it can decode the bytes at all.
 *
 * @tparam A what it writes
 */
trait MessageEncoder[A]:

  /** @return the media type the bytes it writes are in, such as `application/json` */
  def encoding: String

  /**
   * Write a value as it will travel.
   *
   * @param value what to write
   * @return its bytes
   */
  def encode(value: A): Chunk[Byte]
