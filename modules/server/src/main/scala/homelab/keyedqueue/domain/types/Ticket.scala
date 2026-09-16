package homelab.keyedqueue.domain.types

/** A waiter's place in a lock's queue, minted by the store when an enter cannot be granted at once. */
type Ticket = Ticket.Type


object Ticket:
  opaque type Type <: Long = Long

  /**
   * A ticket, trusted.
   *
   * @param value the id as the store minted it
   * @return the ticket
   */
  def apply(value: Long): Type = value
