package homelab.keyedqueue.client.queue


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.messaging.{ Consumer, Producer }
import homelab.keyedqueue.client.queue.Provider.{ BatchConsumerConfig, ConsumerConfig, unreadable }
import homelab.keyedqueue.client.queue.managed.{ ManagedProvider, ManagedSignalConsumer }
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.model.{ Message, MessageId, MessageKey, Ready }
import zio.*


/**
 * The queue as the homelab's messaging ports, so a dkq topology looks like any other.
 *
 * A claim is renewed while its message is being worked and settled however the work ends, which are the
 * two obligations [[QueueClient]] leaves to its caller. What it takes away is the receipt, the batch and
 * the outcome — a caller that wants those drops to the client underneath.
 */
trait Provider:

  /**
   * A consumer of one queue's messages, renewing what it holds for as long as the scope is open.
   *
   * Messages arrive as they travelled. Reading one is the caller's, through `map` or `mapZIO`, which is
   * what puts the decision about a message that will not read where it can be made — retried by letting
   * the failure through, dropped by catching it, sent somewhere else, or counted. [[consumer]] is that
   * composition for the common case.
   *
   * The beat belongs to the scope rather than to a call, because one renewal covers everything this
   * consumer holds. Closing the scope stops it: anything still claimed then lapses on its own lease and
   * is delivered again.
   *
   * @param config which queue to take from, and what this consumer does about waiting and retrying
   * @return the consumer
   */
  def messages(config: ConsumerConfig): URIO[Scope, Consumer[AdapterError, Message.Incoming]]

  /**
   * A consumer of one queue's messages that takes a key's together.
   *
   * Every message of a batch shares one outcome — the logic answered, so all are done, or it did not, so
   * all come back. A caller that needs them settled apart takes [[QueueClient]] underneath, where an
   * outcome is stated per message.
   *
   * @param config which queue to take from, how many of its messages to take at once, and what this
   *               consumer does about waiting and retrying
   * @return the consumer
   */
  def batchedMessages(config: BatchConsumerConfig): URIO[Scope, Consumer.Batched[AdapterError, Message.Incoming]]

  /**
   * The same, reading each message as an `A`.
   *
   * A message that will not read fails the call the way the caller's own logic failing would: it is
   * settled failed and comes back, and the error says what the sender claimed it was. A consumer wanting
   * anything else — dropping it, dead-lettering it, dispatching on what it says it is — composes that
   * over [[messages]] itself.
   *
   * @param config which queue to take from, and what this consumer does about waiting and retrying
   * @tparam A what its messages read as
   * @return the consumer
   */
  def consumer[A: MessageDecoder as decoder](config: ConsumerConfig): URIO[Scope, Consumer[AdapterError, A]] =
    messages(config).map: consumer =>
      consumer.mapZIO: message =>
        ZIO.fromEither(decoder.decode(message)).mapError(unreadable(message))

  /**
   * The same as [[batchedMessages]], reading each message as an `A`.
   *
   * All or nothing, like the outcome: one message nobody can read fails the batch, because a batch handed
   * over in part is a batch answered for in part.
   *
   * @param config which queue to take from, how many at once, and what it does about waiting and retrying
   * @tparam A what its messages read as
   * @return the consumer
   */
  def batched[A: MessageDecoder as decoder](
    config: BatchConsumerConfig
  ): URIO[Scope, Consumer[AdapterError, List[A]]] =
    batchedMessages(config).map: consumer =>
      consumer.mapZIO: messages =>
        ZIO.foreach(messages) { message =>
          ZIO.fromEither(decoder.decode(message)).mapError(unreadable(message))
        }

  /**
   * A consumer of one queue's signals, reading each delivery's key and never its payload.
   *
   * A claim is one key's messages, so the signals in it name one key however many arrived; `logic` runs
   * once for it, and the whole claim settles on what that answers.
   *
   * @param config which queue to take from, how many at once, and how this consumer waits and retries
   * @return the consumer, beating for what it holds until the scope closes
   */
  def signalConsumer(config: BatchConsumerConfig): URIO[Scope, Consumer[AdapterError, Ready]] =
    batchedMessages(config).map: messages =>
      new Consumer[AdapterError, Ready]:
        override def consume[E2 >: AdapterError](logic: Ready => IO[E2, Unit]): IO[E2, Unit] =
          ManagedSignalConsumer(messages).consume: readies =>
            ZIO.foreachDiscard(readies.distinct)(logic)

  /**
   * A producer for one queue, naming each message from the value it sends.
   *
   * The id and the key come from the value because `emit` carries neither: an id derived from what is
   * being sent makes a retried emit the same message rather than a second one, and a key derived the same
   * way is what puts related messages in one order.
   *
   * What its messages are called is the encoder's, which states a name or states that nobody gave one.
   *
   * @param name which queue to send to
   * @tparam A what it sends, which needs a [[Partition]] in scope to name it
   * @return the producer
   */
  def producer[A: {MessageEncoder, Partition}](name: String): UIO[Producer[AdapterError, A]]

  /**
   * The same, for a type whose naming is stated at the call rather than given for the type.
   *
   * @param name which queue to send to
   * @param parts what names a value: its id, and the key whose order it takes its place in
   * @tparam A what it sends
   * @return the producer
   */
  def producerWith[A: MessageEncoder](
    name: String
  )(
    parts: A => (MessageId, MessageKey)
  ): UIO[Producer[AdapterError, A]] =
    given Partition[A] = Partition.from(parts)
    producer(name)

  /**
   * A producer of signals for one queue: a value names the key worth looking at, and carries nothing else.
   *
   * The id is fresh on every emit, so a repeated signal is a second message and not the same one arriving
   * twice. A consumer sees one announcement per call, and a batch of them says what one of them says.
   *
   * @param name which queue to send to
   * @return the producer
   */
  def signalProducer(name: String): UIO[Producer[AdapterError, Ready]]


object Provider:

  /**
   * What a failure to read amounts to in this client's terms.
   *
   * @param message what would not read, which says what its sender claimed it was
   * @param failure what the decoder objected to
   * @return the error to report
   */
  private def unreadable(message: Message.Incoming)(failure: MessageDecoder.Failure): ServiceError =
    ServiceError.Unreadable(s"a message of ${message.payloadType} in ${message.encoding} did not read: $failure")

  /**
   * The messaging ports over a client.
   *
   * Nothing is configured here: what a consumer waits, retries and refuses is a property of the queue it
   * reads, so it is stated per consumer.
   *
   * @param client what every call is made through
   * @return the provider
   */
  def apply(client: QueueClient): Provider = ManagedProvider(client)

  /**
   * What one consumer does, beyond which queue it reads.
   *
   * @param queue which queue to take from
   * @param patience how long a call blocks for work before answering with nothing
   * @param retryAfter how long a key waits before anything this consumer failed is delivered again
   */
  final case class ConsumerConfig(
    queue: String,
    patience: Duration = 20.seconds,
    retryAfter: Duration = Duration.Zero,
  )

  /**
   * What one batched consumer does, beyond which queue it reads.
   *
   * @param queue which queue to take from
   * @param size the most messages to take at once, which the service may lower to its own ceiling
   * @param patience how long a call blocks for work before answering with nothing
   * @param retryAfter how long a key waits before anything this consumer failed is delivered again
   */
  final case class BatchConsumerConfig(
    queue: String,
    size: Int,
    patience: Duration = 20.seconds,
    retryAfter: Duration = Duration.Zero,
  )
