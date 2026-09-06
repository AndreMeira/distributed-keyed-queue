package homelab.keyedqueue.domain.types

/** A claim's generation. A token authorises exactly one transition: grant, revoke and settle all advance it. */
type Token = Token.Type


object Token:
  opaque type Type <: Long = Long

  /**
   * A token, trusted.
   *
   * @param value the counter value the store handed out
   * @return the token
   */
  def apply(value: Long): Type = value
