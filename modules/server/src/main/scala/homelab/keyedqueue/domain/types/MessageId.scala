package homelab.keyedqueue.domain.types

/** A message's own name, unique among those queued for its key: what a settle, or a dedupe, addresses. */
type MessageId = MessageId.Type


object MessageId:
  opaque type Type <: String = String

  /**
   * A message id, trusted.
   *
   * @param value the id as the producer sent it
   * @return the message id
   */
  def apply(value: String): Type = value
