package homelab.keyedqueue.client.queue.managed


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.messaging.Consumer
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.model.{ Message, Ready }
import zio.*


/**
 * One queue's signals, read from the keys they arrived on.
 *
 * The payload is never opened. A signal's key is the whole of what it says, so a delivery is turned into a
 * [[Ready]] by reading its envelope — which is what leaves this with no decoder, and nothing to disagree
 * with the producer about.
 *
 * A batch is one claim's worth, and a claim is one key's messages, so every [[Ready]] in a batch names the
 * same key.
 *
 * @param messages the deliveries this reads, renewed for as long as its scope is open
 */
final private[queue] class ManagedSignalConsumer(
  messages: Consumer.Batched[AdapterError, Message.Incoming]
) extends Consumer.Batched[AdapterError, Ready]:

  /**
   * Hand each claim's signals to `logic`, settling the whole batch on what it answers.
   *
   * @param logic what to do with the keys a claim announced
   * @tparam E2 the error `logic` may abort with, which this passes on
   * @return noop when the intake ends; aborts with whatever `logic` or the delivery aborts with
   */
  override def consume[E2 >: AdapterError](logic: List[Ready] => IO[E2, Unit]): IO[E2, Unit] =
    messages.consume: batch =>
      logic(batch.map(signal))

  /**
   * One delivery as the signal it is.
   *
   * @param message what arrived
   * @return the key it announced
   */
  private def signal(message: Message.Incoming): Ready = Ready(message.key)
