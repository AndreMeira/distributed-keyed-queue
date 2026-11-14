package homelab.keyedqueue.domain.response.queue

import zio.Duration

/**
 * What a schedule recorded.
 *
 * @param scheduled false when a message with this id was already scheduled for the key, so this request was
 *                  ignored: the first one wins, and rescheduling is not offered
 * @param dueIn how long until the recorded schedule is due, on the store's clock: this request's delay when
 *              it was scheduled, the existing schedule's remaining time when it was ignored
 */
final case class ScheduleResponse(scheduled: Boolean, dueIn: Duration)
