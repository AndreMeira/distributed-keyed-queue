package homelab.keyedqueue.client.queue


import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.model.{
  Claim,
  Dequeued,
  Enqueued,
  Message,
  MessageEncoder,
  MessageId,
  MessageKey,
  Receipt,
  Renewed,
  Settled,
  Verdict,
}
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

  import homelab.keyedqueue.client.queue.model.MessageDecoder.auto.given

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
      beaten.succeed(receipts).as(Renewed(Chunk.empty, Instant.EPOCH, lease))

  private def fake(scripted: Dequeued*) =
    for
      answers <- Ref.make(Chunk.fromIterable(scripted))
      settled <- Ref.make(Chunk.empty[Verdict])
      beaten  <- Promise.make[Nothing, Chunk[Receipt]]
    yield Fake(answers, settled, beaten)

  private def consumerOver(
    client: QueueClient,
    policy: Provider.DecodingPolicy = Provider.DecodingPolicy.Retry,
  ) =
    Provider(client).consumer[Order](
      Provider.ConsumerConfig("orders", patience = 1.second, heartbeat = 10.millis, policy = policy)
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
    test("a message that will not read never reaches the logic, and is settled as the policy says") {
      for
        client   <- fake(claim(message(payload = Chunk(9.toByte))))
        consumer <- consumerOver(client, Provider.DecodingPolicy.Discard)
        ran      <- Ref.make(false)
        _        <- consumer.consume(_ => ran.set(true))
        started  <- ran.get
        verdicts <- client.settled.get
      yield assertTrue(!started, verdicts == Chunk(Verdict(MessageId("m1"), Verdict.Outcome.Done)))
    },
    test("discard-after keeps a bad message coming back until it has been tried enough") {
      for
        early    <- fake(claim(message(attempt = 1, payload = Chunk(9.toByte))))
        first    <- consumerOver(early, Provider.DecodingPolicy.DiscardAfter(3))
        _        <- first.consume(_ => ZIO.unit)
        earlyOut <- early.settled.get
        late     <- fake(claim(message(attempt = 3, payload = Chunk(9.toByte))))
        third    <- consumerOver(late, Provider.DecodingPolicy.DiscardAfter(3))
        _        <- third.consume(_ => ZIO.unit)
        lateOut  <- late.settled.get
      yield assertTrue(
        earlyOut == Chunk(Verdict(MessageId("m1"), Verdict.Outcome.Failed)),
        lateOut == Chunk(Verdict(MessageId("m1"), Verdict.Outcome.Done)),
      )
    },
    test("the claim is renewed while the logic is still working") {
      for
        client   <- fake(claim(message()))
        consumer <- consumerOver(client)
        finish   <- Promise.make[Nothing, Unit]
        working  <- consumer.consume(_ => finish.await).fork
        renewed  <- client.beaten.await
        _        <- finish.succeed(())
        _        <- working.join
      yield assertTrue(renewed == Chunk(Receipt("r1")))
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
