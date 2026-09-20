package homelab.keyedqueue.client.queue.model


/**
 * What waiting for work answered.
 */
enum Dequeued:

  /**
   * A key's messages are this caller's until it settles them.
   *
   * @param claim what it holds them by, and what it holds
   */
  case Claimed(claim: Claim)

  /** Nothing became ready before the wait elapsed, which an idle queue answers and is not an error. */
  case Idle
