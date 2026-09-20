package homelab.keyedqueue.client.queue.managed


import homelab.keyedqueue.client.queue.{ Provider, QueueClient }
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.{ MessageDecoder, MessageEncoder, Partition }
import homelab.keyedqueue.client.queue.model.{ Dequeued, Enqueued, Message, MessageId, MessageKey, Receipt, Renewed, Settled, Verdict }
import zio.*
import zio.schema.{ DeriveSchema, Schema }
import zio.test.*

import java.time.Instant


/**
 * What a producer adds over the client: a value names itself, and the envelope is built from what it said.
 */
object ManagedProducerSpec extends ZIOSpecDefault:

  final case class Order(id: String, customer: String)

  private given Schema[Order] = DeriveSchema.gen[Order]

  private given MessageEncoder[Order] = MessageEncoder.deriveAs[Order]("order.v2")

  private val order = Order("o-1", "c-9")

  /** Names an order by its own id, and orders it against the customer it belongs to. */
  private given Partition[Order] with
    override def messageId(value: Order): MessageId   = MessageId(value.id)
    override def messageKey(value: Order): MessageKey = MessageKey(value.customer)

  /** A client that records what it was asked to send. */
  final private class Fake(val sent: Ref[Chunk[(String, Message.Outgoing)]]) extends QueueClient:

    override def enqueue(queue: String, message: Message.Outgoing): IO[ServiceError, Enqueued] =
      sent.update(_ :+ (queue, message)).as(Enqueued(3L))

    override def dequeue(queue: String, maxWait: Duration, maxBatch: Int): IO[ServiceError, Dequeued] =
      ZIO.succeed(Dequeued.Idle)

    override def settle(
      receipt: Receipt,
      verdicts: Chunk[Verdict],
      retryAfter: Duration,
    ): IO[ServiceError, Settled] =
      ZIO.succeed(Settled.Applied)

    override def heartbeat(receipts: Chunk[Receipt]): IO[ServiceError, Renewed] =
      ZIO.succeed(Renewed(Chunk.empty, Instant.EPOCH, 30.seconds))

  private def fake = Ref.make(Chunk.empty[(String, Message.Outgoing)]).map(Fake(_))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ManagedProducer")(
    test("a value is sent under the name and key it gives for itself") {
      for
        client   <- fake
        producer <- Provider(client).producerWith[Order]("orders")(value => MessageId(value.id) -> MessageKey(value.customer))
        _        <- producer.emit(order)
        sent     <- client.sent.get
      yield assertTrue(
        sent.map(_._1) == Chunk("orders"),
        sent.map(_._2.id) == Chunk(MessageId("o-1")),
        sent.map(_._2.key) == Chunk(MessageKey("c-9")),
        sent.map(_._2.payloadType) == Chunk("order.v2"),
        sent.map(_._2.encoding) == Chunk(MessageEncoder.protobuf),
      )
    },
    test("the same value sends the same id, so a repeated emit is one message rather than two") {
      for
        client   <- fake
        producer <- Provider(client).producer[Order]("orders")
        _        <- producer.emit(order) *> producer.emit(order)
        sent     <- client.sent.get
      yield assertTrue(sent.map(_._2.id).distinct == Chunk(MessageId("o-1")), sent.length == 2)
    },
    test("the bytes are the encoder's, and read back as the value") {
      for
        client   <- fake
        producer <- Provider(client).producer[Order]("orders")
        _        <- producer.emit(order)
        sent     <- client.sent.get
        arrived   = sent.map(outgoing =>
                      Message.Incoming(
                        outgoing._2.key,
                        outgoing._2.id,
                        outgoing._2.payloadType,
                        outgoing._2.encoding,
                        outgoing._2.payload,
                        Instant.EPOCH,
                        attempt = 1,
                      )
                    )
      yield assertTrue(arrived.map(MessageDecoder.derive[Order].decode) == Chunk(Right(order)))
    },
    test("emitMany sends each in order") {
      for
        client   <- fake
        producer <- Provider(client).producer[Order]("orders")
        _        <- producer.emitMany(List(order, Order("o-2", "c-9")))
        sent     <- client.sent.get
      yield assertTrue(sent.map(_._2.id) == Chunk(MessageId("o-1"), MessageId("o-2")))
    },
  )
