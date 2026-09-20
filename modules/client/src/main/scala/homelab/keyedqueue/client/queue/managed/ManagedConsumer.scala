package homelab.keyedqueue.client.queue.managed


import homelab.common.messaging.Consumer
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.model.Message
import homelab.keyedqueue.client.queue.{ Provider, QueueClient }
import zio.*


/**
 * One queue's messages, handed to a caller's logic one at a time.
 *
 * A batch of one, which is what a single message is: the claim is kept alive by the beat while the logic
 * runs and settled however the work ends, and the all-or-nothing an outcome shared across a batch implies
 * is no constraint at all over one message.
 *
 * @param client what the calls are made through
 * @param heartbeat what keeps the claim alive while the logic runs
 * @param conf which queue it reads, and how it waits and retries
 */
final private[queue] class ManagedConsumer(
  client: QueueClient,
  heartbeat: Heartbeat,
  conf: Provider.ConsumerConfig,
) extends Consumer[ServiceError, Message.Incoming]:

  /** The claim this reads through, which takes one message at a time. */
  private val batch = ManagedBatch(
    client,
    heartbeat,
    Provider.BatchConsumerConfig(
      queue = conf.queue,
      size = 1,
      patience = conf.patience,
      retryAfter = conf.retryAfter,
      heartbeat = conf.heartbeat,
    ),
  )

  /**
   * Claim one message and work it, or answer with nothing when none became ready.
   *
   * @param logic what to run on the message
   * @tparam E2 what the logic aborts with, alongside this client's own failures
   * @return noop once the message is settled, or once the wait elapsed with nothing to take; aborts with
   *         what the logic aborted with, or with a [[ServiceError]] when a call does not land
   */
  override def consume[E2 >: ServiceError](logic: Message.Incoming => IO[E2, Unit]): IO[E2, Unit] =
    batch.consume(each(logic))

  /**
   * The logic over what a claim of one holds.
   *
   * @param logic what to run on the message
   * @param messages what the claim holds, which a batch of one holds a single one of
   * @tparam E2 what the logic aborts with
   * @return what the logic answered
   */
  private def each[E2 >: ServiceError](
    logic: Message.Incoming => IO[E2, Unit]
  )(
    messages: List[Message.Incoming]
  ): IO[E2, Unit] =
    ZIO.foreachDiscard(messages)(logic)
