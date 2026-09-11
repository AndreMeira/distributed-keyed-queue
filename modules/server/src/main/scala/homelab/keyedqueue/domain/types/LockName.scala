package homelab.keyedqueue.domain.types

/** The name of a lock: what mutual exclusion is defined over, one holder at a time across every instance. */
type LockName = LockName.Type


object LockName:
  opaque type Type <: String = String

  /**
   * A lock name, trusted.
   *
   * @param value the name as given
   * @return the lock name
   */
  def apply(value: String): Type = value
