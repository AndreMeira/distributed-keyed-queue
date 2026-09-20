package homelab.keyedqueue.client.queue


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.messaging.{ Consumer, Producer }
import homelab.keyedqueue.client.queue
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
   * @param config which queue to take from, and how this consumer waits, retries and refuses
   * @tparam A what its messages read as
   * @return the consumer, beating for what it holds until the scope closes
   */
  override def consumer[A: MessageDecoder as decoder](config: Provider.ConsumerConfig): URIO[Scope, Consumer[AdapterError, A]] =
    for
      heartbeat <- Heartbeat.make(client, config.heartbeat)
      _         <- heartbeat.start.forkScoped
    yield ManagedConsumer(client, heartbeat, config)

  /**
   * @param name which queue to send to
   * @tparam A what it sends
   * @return the producer
   */
  override def producer[A: MessageEncoder](name: String): UIO[Producer[AdapterError, A]] = ???
