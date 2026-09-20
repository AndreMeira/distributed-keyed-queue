package homelab.keyedqueue.client.queue.managed


import homelab.keyedqueue.client.queue.{ Provider, QueueClient }
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.{ MessageDecoder, MessageEncoder }
import homelab.keyedqueue.client.queue.model.{ Claim, Dequeued, Enqueued, Message, MessageId, MessageKey, Receipt, Renewed, Settled, Verdict }
import zio.*
import zio.schema.{ DeriveSchema, Schema }
import zio.test.*

import java.time.Instant


/**
 * What the managed form promises beyond the client: a claim is renewed while its message is worked, and
 * settled however the work ends.
 */
object ManagedConsumerSpec extends ZIOSpecDefault:

  final case class Order(id: String)

  private given Schema[Order] = DeriveSchema.gen[Order]

  import homelab.keyedqueue.client.queue.MessageDecoder.auto.given

  private val lease   = 40.millis
  private val encoder = MessageEncoder.derive[Order]
  private val order   = Order("o-1")

  private def message(attempt: Int = 1, payload: Chunk[Byte] = encoder.encode(order)): Message.Incoming =
    Message.Incoming(MessageKey("k1"), MessageId("m1"), "order.v1", encoder.encoding, payload, Instant.EPOCH, attempt)

  private def claim(of: Message.Incoming): Dequeued =
    Dequeued.Claimed(Claim(Receipt("r1"), NonEmptyChunk(of), Instant.EPOCH, lease, backlogDepth = 0))

  /** A client that hands out scripted answers and records what it was told. */
  final private class Fake(
    answers: Ref[Chunk[Dequeued]],
    val settled: Ref[Chunk[Verdict]],
    val beaten: Promise[Nothing, Chunk[Receipt]],
    val beats: Ref[Int],
  ) extends QueueClient:

    override def enqueue(queue: String, message: Message.Outgoing): IO[ServiceError, Enqueued] =
      ZIO.succeed(Enqueued(0L))

    override def dequeue(queue: String, maxWait: Duration, maxBatch: Int): IO[ServiceError, Dequeued] =
      answers.modify {
        case answer +: rest => (answer, rest)
        case _              => (Dequeued.Idle, Chunk.empty)
      }

    override def settle(
      receipt: Receipt,
      verdicts: Chunk[Verdict],
      retryAfter: Duration,
    ): IO[ServiceError, Settled] =
      settled.update(_ ++ verdicts).as(Settled.Applied)

    override def heartbeat(receipts: Chunk[Receipt]): IO[ServiceError, Renewed] =
      beats.update(_ + 1) *> beaten.succeed(receipts).as(Renewed(Chunk.empty, Instant.EPOCH, lease))

  private def fake(scripted: Dequeued*) =
    for
      answers <- Ref.make(Chunk.fromIterable(scripted))
      settled <- Ref.make(Chunk.empty[Verdict])
      beaten  <- Promise.make[Nothing, Chunk[Receipt]]
      beats   <- Ref.make(0)
    yield Fake(answers, settled, beaten, beats)

  private def consumerOver(client: QueueClient) =
    Provider(client).consumer[Order](
      Provider.ConsumerConfig("orders", patience = 1.second)
    )

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ManagedConsumer")(
    test("a message that reads is handed over, and settled done when the logic answers") {
      for
        client   <- fake(claim(message()))
        consumer <- consumerOver(client)
        worked   <- Ref.make(Chunk.empty[Order])
        _        <- consumer.consume(value => worked.update(_ :+ value))
        seen     <- worked.get
        verdicts <- client.settled.get
      yield assertTrue(seen == Chunk(order), verdicts == Chunk(Verdict(MessageId("m1"), Verdict.Outcome.Done)))
    },
    test("a logic that fails settles the message failed, and its error is what surfaces") {
      for
        client   <- fake(claim(message()))
        consumer <- consumerOver(client)
        outcome  <- consumer.consume(_ => ZIO.fail(ServiceError.Rejected("mine"))).either
        verdicts <- client.settled.get
      yield assertTrue(
        outcome == Left(ServiceError.Rejected("mine")),
        verdicts == Chunk(Verdict(MessageId("m1"), Verdict.Outcome.Failed)),
      )
    },
    test("nothing ready means the logic never runs and nothing is settled") {
      for
        client   <- fake(Dequeued.Idle)
        consumer <- consumerOver(client)
        ran      <- Ref.make(false)
        _        <- consumer.consume(_ => ran.set(true))
        started  <- ran.get
        verdicts <- client.settled.get
      yield assertTrue(!started, verdicts.isEmpty)
    },
    test("a message that will not read fails the call and comes back") {
      // Reading is the caller's, so a message it cannot take fails the reading — a handler failure like
      // any other, settled the way one is.
      for
        client   <- fake(claim(message(payload = Chunk(9.toByte))))
        consumer <- consumerOver(client)
        ran      <- Ref.make(false)
        outcome  <- consumer.consume(_ => ran.set(true)).either
        started  <- ran.get
        verdicts <- client.settled.get
      yield assertTrue(
        !started,
        outcome.left.exists(_.isInstanceOf[ServiceError.Unreadable]),
        verdicts == Chunk(Verdict(MessageId("m1"), Verdict.Outcome.Failed)),
      )
    },
    test("a caller that answers for an unreadable message drops it instead") {
      // What `Discard` used to be, composed: the reading answers, so the message settles done and is
      // gone. `DiscardAfter` is the same edit with a look at the attempt the delivery carries.
      for
        client   <- fake(claim(message(payload = Chunk(9.toByte))))
        raw      <- Provider(client).messages(
                      Provider.ConsumerConfig("orders", patience = 1.second)
                    )
        _        <- raw.consume(dropping)
        verdicts <- client.settled.get
      yield assertTrue(verdicts == Chunk(Verdict(MessageId("m1"), Verdict.Outcome.Done)))
    },
    test("the claim is renewed while the logic is still working, on the lease it was granted") {
      for
        client   <- fake(claim(message()))
        consumer <- consumerOver(client)
        finish   <- Promise.make[Nothing, Unit]
        working  <- consumer.consume(_ => finish.await).fork
        renewed  <- client.beaten.await.timeout(lease * 2)
        _        <- finish.succeed(())
        _        <- working.join
      yield assertTrue(renewed.contains(Chunk(Receipt("r1"))))
    },
    test("a claim taken after the beat stood down starts it again") {
      for
        client   <- fake(claim(message()), claim(message()))
        consumer <- consumerOver(client)
        first    <- Promise.make[Nothing, Unit]
        one      <- consumer.consume(_ => first.await).fork
        _        <- client.beaten.await
        _        <- first.succeed(())
        _        <- one.join
        _        <- ZIO.sleep(lease)
        stood    <- client.beats.get
        second   <- Promise.make[Nothing, Unit]
        two      <- consumer.consume(_ => second.await).fork
        _        <- ZIO.sleep(lease)
        again    <- client.beats.get
        _        <- second.succeed(())
        _        <- two.join
      yield assertTrue(again > stood)
    },
    test("a consumer interrupted mid-message still says what became of it") {
      for
        client   <- fake(claim(message()))
        consumer <- consumerOver(client)
        inside   <- Promise.make[Nothing, Unit]
        working  <- consumer.consume(_ => inside.succeed(()) *> ZIO.never).fork
        _        <- inside.await
        _        <- working.interrupt
        verdicts <- client.settled.get.repeatUntil(_.nonEmpty)
      yield assertTrue(verdicts == Chunk(Verdict(MessageId("m1"), Verdict.Outcome.Failed)))
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)

  /** A caller's own reading, which answers for a message it cannot take rather than failing. */
  private def dropping(message: Message.Incoming): UIO[Unit] =
    ZIO.succeed(MessageDecoder[Order].decode(message)).unit
