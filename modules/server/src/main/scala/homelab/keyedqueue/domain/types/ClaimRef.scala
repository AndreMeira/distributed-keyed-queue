package homelab.keyedqueue.domain.types

/**
 * The opaque handle a consumer holds while it works a message: a
 * [[homelab.keyedqueue.domain.model.Claim]] it cannot read.
 */
type ClaimRef = ClaimRef.Type


object ClaimRef:
  opaque type Type <: String = String

  /**
   * A reference, trusted.
   *
   * @param value the encoded handle
   * @return the reference
   */
  def apply(value: String): Type = value
