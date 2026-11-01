package homelab.keyedqueue.client.queue.managed


import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.{ MessageDecoder, MessageEncoder, Provider, QueueClient }
import homelab.keyedqueue.client.queue.model.{ Dequeued, Enqueued, Message, MessageKey, Ready, Receipt, Renewed, Settled, Verdict }
import zio.*
import zio.schema.{ DeriveSchema, Schema }
import zio.test.*

import java.time.Instant


/**
 * What a signal producer promises over a plain one: a signal names a key and carries nothing else, and two
 * signals are two messages, never one deduplicated away.
 */
object ManagedSignalProducerSpec extends ZIOSpecDefault:
  import Support.{ *, given }

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ManagedSignalProducer")(
    test("a signal is sent to its queue, keyed by the key it names") {
      for
        client   <- fake
        producer <- Provider(client).signalProducer("signals")
        _        <- producer.emit(Ready("k1"))
        sent     <- client.sent.get
      yield assertTrue(sent.map(_._1) == Chunk("signals"), sent.map(_._2.key) == Chunk(MessageKey("k1")))
    },
    test("repeated signals for one key get distinct ids, so the queue keeps both rather than deduplicating") {
      // Fails with one id if a signal is ever named from its value, as a plain producer's messages are: the
      // queue would then drop the second as a repeat, and an announcement made during a claim would be lost.
      for
        client   <- fake
        producer <- Provider(client).signalProducer("signals")
        _        <- producer.emit(Ready("k1")) *> producer.emit(Ready("k1"))
        sent     <- client.sent.get
      yield assertTrue(sent.length == 2, sent.map(_._2.id).distinct.length == 2, sent.map(_._2.key).distinct == Chunk(MessageKey("k1")))
    },
    test("the Ready is encoded under the unnamed payload type, and reads back") {
      for
        client   <- fake
        producer <- Provider(client).signalProducer("signals")
        _        <- producer.emit(Ready("k1"))
        sent     <- client.sent.get
        arrived   = sent.map((_, out) => Message.Incoming(out.key, out.id, out.payloadType, out.encoding, out.payload, Instant.EPOCH, attempt = 1))
      yield assertTrue(
        sent.map(_._2.payloadType) == Chunk(MessageEncoder.unnamed),
        sent.map(_._2.encoding) == Chunk(MessageEncoder.protobuf),
        arrived.map(MessageDecoder.derive[Ready].decode) == Chunk(Right(Ready("k1"))),
      )
    },
  )

  private object Support:

    given Schema[Ready] = DeriveSchema.gen[Ready]

    /**
     * A client that records what it was asked to send, and answers everything else with nothing.
     *
     * @param sent every queue name and message enqueued, in order
     */
    final class Fake(val sent: Ref[Chunk[(String, Message.Outgoing)]]) extends QueueClient:

      override def enqueue(queue: String, message: Message.Outgoing): IO[ServiceError, Enqueued] =
        sent.update(_ :+ (queue, message)).as(Enqueued(1L))

      override def dequeue(queue: String, maxWait: Duration, maxBatch: Int): IO[ServiceError, Dequeued] =
        ZIO.succeed(Dequeued.Idle)

      override def settle(receipt: Receipt, verdicts: Chunk[Verdict], retryAfter: Duration): IO[ServiceError, Settled] =
        ZIO.succeed(Settled.Applied)

      override def heartbeat(receipts: Chunk[Receipt]): IO[ServiceError, Renewed] =
        ZIO.succeed(Renewed(Chunk.empty, Instant.EPOCH, 30.seconds))

    /**
     * A fresh recording client.
     *
     * @return the client, having sent nothing
     */
    def fake: UIO[Fake] = Ref.make(Chunk.empty[(String, Message.Outgoing)]).map(Fake(_))
