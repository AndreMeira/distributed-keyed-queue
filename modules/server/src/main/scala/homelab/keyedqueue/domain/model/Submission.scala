package homelab.keyedqueue.domain.model

import homelab.keyedqueue.domain.types.QueueName

/**
 * A message accepted for a queue: where it goes, and what it is.
 *
 * '''The trusted counterpart of `QueueRequest.Enqueue`.''' The request holds two strings and cargo; this
 * holds a [[QueueName]] and a [[Message]] whose key and id are named. Obtainable only from the parse, so a
 * caller of the store cannot have skipped it.
 *
 * The queue and the message stay separate rather than the message carrying its queue: a queue name is the
 * address a message was sent to, not a property of the message — which is why the same message may be
 * submitted to two queues and remain one message.
 *
 * @param queue where it was sent
 * @param message what was sent, ordered by its own key
 */
final case class Submission(queue: QueueName, message: Message)
