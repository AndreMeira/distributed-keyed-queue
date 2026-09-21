package homelab.keyedqueue.client.queue.managed


import homelab.common.messaging.Producer
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.managed.ManagedSignalProducer.given
import homelab.keyedqueue.client.queue.{ MessageEncoder, QueueClient }
import homelab.keyedqueue.client.queue.model.{ MessageId, MessageKey, Ready }
import zio.{ Chunk, Random }
import zio.schema.{ DeriveSchema, Schema }


class ManagedSignalProducer(client: QueueClient, queue: String) extends Producer[ServiceError, Ready] {

  override def emit(value: Ready): zio.IO[ServiceError, Unit] =
    for {
      id     <- Random.nextUUID.map(_.toString)
      message = MessageEncoder.message(MessageKey(value.id), MessageId(id), value)
      _      <- client.enqueue(queue, message)
    } yield ()

}


object ManagedSignalProducer {
  given MessageEncoder[Ready] = {
    val schema = DeriveSchema.gen[Ready]
    MessageEncoder.derive[Ready](using schema)
  }
}
