package homelab.keyedqueue.domain.model


import homelab.keyedqueue.domain.types.QueueName
import zio.Duration


/**
 * A caller's demand for work: where from, how long it will wait, and how much it will take.
 *
 * '''The trusted counterpart of `QueueRequest.Dequeue`, and bounded by construction.''' The request says
 * what a caller asked for; this says what the service agreed to, with both numbers already inside the
 * limits it enforces. Obtainable only from the parse, so the store cannot be handed an hour-long wait or a
 * batch of ten thousand.
 *
 * The fields are named for what they mean here rather than for what the caller called them: `maxWait`
 * became `patience` and `maxBatch` became `batch`, because a maximum is what you ask for and this is what
 * was granted.
 *
 * @param queue the queue to take from
 * @param patience how long this claim will wait for work, at most what the service allows
 * @param batch the most messages it will take, at least one and at most what the service allows
 */
final case class Demand(queue: QueueName, patience: Duration, batch: Int)
