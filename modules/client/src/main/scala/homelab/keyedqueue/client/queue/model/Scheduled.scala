package homelab.keyedqueue.client.queue.model

import zio.Duration


/**
 * What scheduling a message answered.
 *
 * Either way the message will arrive: the two cases differ in which schedule it arrives by.
 */
enum Scheduled:

  /**
   * This request was recorded.
   *
   * @param dueIn how long until the message falls due, on the service's clock: the delay asked for
   */
  case Recorded(dueIn: Duration)

  /**
   * A message with this id was already scheduled for the key, so this request changed nothing: the first
   * schedule wins, and rescheduling is not offered. To move a deadline, schedule a new message with its own id.
   *
   * @param dueIn how long until the earlier schedule falls due, on the service's clock
   */
  case Ignored(dueIn: Duration)
