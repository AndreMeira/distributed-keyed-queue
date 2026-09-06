package homelab.keyedqueue.domain.types

/** What a message is ordered by: one key is worked by one consumer at a time, keys run concurrently. */
type MessageKey = MessageKey.Type


object MessageKey:
  opaque type Type <: String = String

  /**
   * A key, trusted.
   *
   * @param value the key as given
   * @return the message key
   */
  def apply(value: String): Type = value
