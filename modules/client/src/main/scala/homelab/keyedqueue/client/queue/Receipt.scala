package homelab.keyedqueue.client.queue

/**
 * The opaque handle a claim is settled and renewed by, and nothing a caller reads.
 */
type Receipt = Receipt.Type


object Receipt:
  opaque type Type <: String = String

  /**
   * A handle as the service issued it.
   *
   * @param value the encoded handle
   * @return the receipt
   */
  def apply(value: String): Type = value
