package homelab.keyedqueue.client.queue.managed


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.messaging.{ Consumer, Producer }
import homelab.keyedqueue.client.queue
import homelab.keyedqueue.client.queue.model.{ Message, MessageEncoder, MessageId, MessageKey }
import homelab.keyedqueue.client.queue.{ Provider, QueueClient }
import zio.*


/**
 * The messaging ports over a client.
 *
 * Each consumer gets a registry and a beat of its own, so what it holds is renewed for as long as it is
 * open and dies with it. Several consumers renew separately, which is a call each rather than one: the
 * service renews the receipts a beat names and leaves the rest to their own leases.
 *
 * @param client what every call is made through
 */
final private[client] class ManagedProvider(client: QueueClient) extends Provider:

  /**
   * @param config which queue to take from, and how this consumer waits and retries
   * @return the consumer, beating for what it holds until the scope closes
   */
  override def messages(
    config: Provider.ConsumerConfig
  ): URIO[Scope, Consumer[AdapterError, Message.Incoming]] =
    for
      heartbeat <- Heartbeat.make(client, config.heartbeat)
      _         <- heartbeat.start.forkScoped
    yield ManagedConsumer(client, heartbeat, config)

  /**
   * @param config which queue to take from, how many at once, and how this consumer waits and retries
   * @return the consumer, beating for what it holds until the scope closes
   */
  override def batchedMessages(
    config: Provider.BatchConsumerConfig
  ): URIO[Scope, Consumer.Batched[AdapterError, Message.Incoming]] =
    for
      heartbeat <- Heartbeat.make(client, config.heartbeat)
      _         <- heartbeat.start.forkScoped
    yield ManagedBatch(client, heartbeat, config)

  /**
   * @param name which queue to send to
   * @param parts what names a value: its id, and the key whose order it takes its place in
   * @tparam A what it sends
   * @return the producer
   */
  override def producerWith[A: MessageEncoder](
    name: String
  )(
    parts: A => (MessageId, MessageKey)
  ): UIO[Producer[AdapterError, A]] =
    ZIO.succeed(ManagedProducer(client, name, parts))
