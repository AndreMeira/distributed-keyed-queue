package homelab.keyedqueue.client.queue


import homelab.common.messaging.Producer
import homelab.keyedqueue.client.ServiceError
import zio.*


/**
 * One queue's messages, sent from the values a caller hands over.
 *
 * What it adds over [[QueueClient]] is the envelope: the value names itself, and the encoder writes it
 * and says what it is and what format it wrote. What it takes away is the depth a send answers with,
 * which is a measure rather than a decision.
 *
 * @param client what the calls are made through
 * @param queue which queue it sends to
 * @param parts what names a value: its id, and the key whose order it takes its place in
 * @param encoder what writes the value, and what names the format it was written in
 * @tparam A what it sends
 */
final private[queue] class ManagedProducer[A: MessageEncoder as encoder](
  client: QueueClient,
  queue: String,
  parts: A => (MessageId, MessageKey),
) extends Producer[ServiceError, A]:

  /**
   * Send one value.
   *
   * @param value what to send
   * @return noop once the service has it; aborts with a [[ServiceError]] when the call does not land
   */
  override def emit(value: A): IO[ServiceError, Unit] =
    val (id, key) = parts(value)
    client.enqueue(queue, Message.Outgoing(key, id, value)).unit
