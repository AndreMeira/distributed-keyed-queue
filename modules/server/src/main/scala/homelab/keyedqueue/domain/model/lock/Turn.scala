package homelab.keyedqueue.domain.model.lock

import zio.Duration


/**
 * Whether a ticket's turn has come.
 */
enum Turn:

  /**
   * The turn came and the lock is this caller's.
   *
   * @param hold what authorises releasing and refreshing it
   */
  case Granted(hold: Hold)

  /**
   * Not yet.
   *
   * @param recheck how long until the answer can change
   */
  case Wait(recheck: Duration)

  /**
   * The queue no longer knows this ticket, so the caller has lost its place and must enter again.
   */
  case Gone
