package homelab.keyedqueue.domain.request.queue

import zio.Duration

/**
 * A caller's ask to append a message to a key, not now but after a delay.
 *
 * The message is stated exactly as for an enqueue, and checked the same way: until it falls due it waits
 * outside its key's line, and from then on it is an ordinary message of its key.
 *
 * @param queue the queue to append to, as it arrived
 * @param message the message, as it arrived
 * @param deliverAfter how long from now it becomes claimable, as it arrived; measured on the store's clock
 */
final case class ScheduleRequest(queue: String, message: EnqueueRequest.Message, deliverAfter: Duration)
