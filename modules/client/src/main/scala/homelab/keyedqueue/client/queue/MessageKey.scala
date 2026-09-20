package homelab.keyedqueue.client.queue

/**
 * What a message is ordered within: the one thing a claim owns, and the reason two consumers never work
 * the same messages at once.
 *
 * An empty key is a key of its own, so a message sent without one is ordered against nothing.
 */
type MessageKey = MessageKey.Type


object MessageKey:
  opaque type Type <: String = String

  /**
   * A key as a producer chose it, or as a delivery carried it.
   *
   * @param value the key
   * @return the message key
   */
  def apply(value: String): Type = value
