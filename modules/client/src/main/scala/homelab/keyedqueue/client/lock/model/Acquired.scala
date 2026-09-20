package homelab.keyedqueue.client.lock.model


/**
 * What asking for a lock answered.
 */
enum Acquired:

  /**
   * The lock is this caller's.
   *
   * @param hold what it holds it by
   */
  case Granted(hold: Hold)

  /** Somebody else has it: the wait elapsed, or a try found it taken. */
  case Unavailable
