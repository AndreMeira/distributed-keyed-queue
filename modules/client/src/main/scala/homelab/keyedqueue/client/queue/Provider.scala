package homelab.keyedqueue.client.queue


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.messaging.{ Consumer, Producer }
import homelab.keyedqueue.client.queue.Provider.{ BatchConsumerConfig, ConsumerConfig, Partition }
import homelab.keyedqueue.client.queue.managed.ManagedProvider
import homelab.keyedqueue.client.queue.model.{ MessageDecoder, MessageEncoder, MessageId, MessageKey }
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
   * A consumer of one queue, renewing what it holds for as long as the scope is open.
   *
   * The beat belongs to the scope rather than to a call, because one renewal covers everything this
   * consumer holds. Closing the scope stops it: anything still claimed then lapses on its own lease and
   * is delivered again.
   *
   * @param config which queue to take from, and what this consumer does about waiting, retrying and
   *               messages it cannot read
   * @tparam A what its messages read as
   * @return the consumer
   */
  def consumer[A: MessageDecoder as decoder](config: ConsumerConfig): URIO[Scope, Consumer[AdapterError, A]]

  /**
   * A consumer of one queue that takes a key's messages together.
   *
   * Every message of a batch shares one outcome — the logic answered, so all are done, or it did not, so
   * all come back. A caller that needs them settled apart takes [[QueueClient]] underneath, where an
   * outcome is stated per message.
   *
   * @param config which queue to take from, how many of its messages to take at once, and what this
   *               consumer does about waiting, retrying and messages it cannot read
   * @tparam A what its messages read as
   * @return the consumer
   */
  def batched[A: MessageDecoder as decoder](
    config: BatchConsumerConfig
  ): URIO[Scope, Consumer.Batched[AdapterError, A]]

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
   * @param parts what names a value: its id, and the key whose order it takes its place in
   * @tparam A what it sends
   * @return the producer
   */
  def producerWith[A: MessageEncoder](
    name: String
  )(
    parts: A => (MessageId, MessageKey)
  ): UIO[Producer[AdapterError, A]]

  /**
   * The same, for a type that says how it is named.
   *
   * @param name which queue to send to
   * @tparam A what it sends, which needs a [[Provider.Partition]] in scope to name it
   * @return the producer
   */
  def producer[A: {MessageEncoder, Partition as partition}](name: String): UIO[Producer[AdapterError, A]] =
    producerWith(name)(value => partition.messageId(value) -> partition.messageKey(value))


object Provider:

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
   * @param heartbeat how often to beat before a claim has stated a lease to go by; once one has, the
   *                  lease it granted is what times the beats, and this no longer applies
   * @param policy what to do with a message that cannot be read
   */
  final case class ConsumerConfig(
    queue: String,
    patience: Duration = 20.seconds,
    retryAfter: Duration = Duration.Zero,
    heartbeat: Duration = 5.seconds,
    policy: DecodingPolicy = DecodingPolicy.DiscardAfter(3),
  )

  /**
   * What one batched consumer does, beyond which queue it reads.
   *
   * @param queue which queue to take from
   * @param size the most messages to take at once, which the service may lower to its own ceiling
   * @param patience how long a call blocks for work before answering with nothing
   * @param retryAfter how long a key waits before anything this consumer failed is delivered again
   * @param heartbeat how often to beat before a claim has stated a lease to go by; once one has, the
   *                  lease it granted is what times the beats, and this no longer applies
   * @param policy what to do with a message that cannot be read
   */
  final case class BatchConsumerConfig(
    queue: String,
    size: Int,
    patience: Duration = 20.seconds,
    retryAfter: Duration = Duration.Zero,
    heartbeat: Duration = 5.seconds,
    policy: DecodingPolicy = DecodingPolicy.DiscardAfter(3),
  )

  /**
   * What a consumer does with a message it cannot read.
   *
   * The service has no dead letter, so the choice is between a message coming back and a message going
   * away, and neither is right everywhere: what is poison to one consumer is a type another one handles.
   * Stated where a consumer is built, because the messaging port a handler is given has no channel to
   * report it through.
   */
  enum DecodingPolicy:

    /** Settle it failed, so it returns to its key's order and arrives again. */
    case Retry

    /** Settle it done, so it is gone. */
    case Discard

    /**
     * Retry it until it has been delivered this often, then discard it.
     *
     * The only one of the three that neither loses a message on its first bad read nor blocks its key
     * forever, because a delivery carries how many times it has been tried.
     *
     * @param attempts how many deliveries to allow before discarding
     */
    case DiscardAfter(attempts: Int)

  /**
   * How a value says what to call it and where it belongs.
   *
   * The two things `emit` cannot carry, for a caller that would rather state them once for a type than at
   * every producer it builds.
   *
   * @tparam A what it names
   */
  trait Partition[A]:

    /**
     * What a settle will name this message by, and what makes a repeated send one message rather than two.
     *
     * @param value what is being sent
     * @return its id
     */
    def messageId(value: A): MessageId

    /**
     * The key whose order this message takes its place in.
     *
     * @param value what is being sent
     * @return its key
     */
    def messageKey(value: A): MessageKey

  object Partition:

    /**
     * The naming a caller has in scope for a type.
     *
     * @tparam A what it names, which needs a partition in scope
     * @return that naming
     */
    def apply[A: Partition as partition]: Partition[A] = partition
