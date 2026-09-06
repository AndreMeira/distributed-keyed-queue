package homelab.keyedqueue.domain.types

/** The queue a message was addressed to. It is the address, never part of the message. */
type QueueName = QueueName.Type


object QueueName:
  opaque type Type <: String = String

  /**
   * A queue name, trusted.
   *
   * @param value the name as given
   * @return the queue name
   */
  def apply(value: String): Type = value
