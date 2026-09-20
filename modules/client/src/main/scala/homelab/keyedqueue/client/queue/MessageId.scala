package homelab.keyedqueue.client.queue

/**
 * What a message is named by within its key: what a settle echoes back, and what makes a repeated enqueue
 * one message rather than two.
 */
type MessageId = MessageId.Type


object MessageId:
  opaque type Type <: String = String

  /**
   * An id as a producer chose it, or as a delivery carried it.
   *
   * @param value the id
   * @return the message id
   */
  def apply(value: String): Type = value
