package homelab.keyedqueue.domain.model.lock


import homelab.keyedqueue.domain.types.Ticket
import zio.Duration


/**
 * Where a caller stands after asking for a lock: holding it, or queued for it.
 */
enum Position:

  /**
   * The lock was free and is now this caller's.
   *
   * @param hold what authorises releasing and refreshing it
   */
  case Granted(hold: Hold)

  /**
   * Somebody else has it, and this caller is queued behind them.
   *
   * @param ticket this caller's place
   * @param recheck how long until the answer can change, whatever the wakes say
   */
  case Queued(ticket: Ticket, recheck: Duration)
