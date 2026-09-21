package homelab.keyedqueue.client.queue.managed


import homelab.common.messaging.Producer
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.managed.ManagedSignalProducer.given
import homelab.keyedqueue.client.queue.{ MessageEncoder, QueueClient }
import homelab.keyedqueue.client.queue.model.{ MessageId, MessageKey, Ready }
import zio.{ Chunk, Random }
import zio.schema.{ DeriveSchema, Schema }


/**
 * One queue's signals, each naming the key a consumer should look at.
 *
 * What it adds over [[QueueClient]] is the envelope and the naming: the key comes from the value, and the
 * id is minted per call, so emitting the same [[Ready]] twice queues two messages.
 *
 * @param client what the calls are made through
 * @param queue which queue it sends to
 */
class ManagedSignalProducer(client: QueueClient, queue: String) extends Producer[ServiceError, Ready] {

  /**
   * Send one signal.
   *
   * @param value the key to announce
   * @return noop once the service has it; aborts with a [[ServiceError]] when the call does not land
   */
  override def emit(value: Ready): zio.IO[ServiceError, Unit] =
    for {
      id     <- Random.nextUUID.map(_.toString)
      message = MessageEncoder.message(MessageKey(value.id), MessageId(id), value)
      _      <- client.enqueue(queue, message)
    } yield ()

}


object ManagedSignalProducer {

  /** What signals are written with; what it writes states [[MessageEncoder.unnamed]] as its payload type. */
  given MessageEncoder[Ready] = {
    val schema = DeriveSchema.gen[Ready]
    MessageEncoder.derive[Ready](using schema)
  }
}
