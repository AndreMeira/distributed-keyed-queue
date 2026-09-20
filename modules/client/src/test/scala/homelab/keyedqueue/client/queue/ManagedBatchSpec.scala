package homelab.keyedqueue.client.queue


import homelab.keyedqueue.client.ServiceError
import zio.*
import zio.schema.{ DeriveSchema, Schema }
import zio.test.*

import java.time.Instant


/**
 * What a batch promises beyond one message at a time: every message of a claim shares an outcome.
 *
 * The cases worth testing are the ones where they disagree — a batch the logic failed halfway through, and
 * a batch carrying a message that never reached the logic at all.
 */
object ManagedBatchSpec extends ZIOSpecDefault:

  final case class Order(id: String)

  private given Schema[Order] = DeriveSchema.gen[Order]

  import MessageDecoder.auto.given

  private val lease   = 40.millis
  private val encoder = MessageEncoder.derive[Order]

  private def order(id: String): Order = Order(id)

  private def message(id: String, payload: Chunk[Byte], attempt: Int = 1): Message.Incoming =
    Message.Incoming("k1", MessageId(id), "order.v1", encoder.encoding, payload, Instant.EPOCH, attempt)

  private def readable(id: String): Message.Incoming =
    message(id, encoder.encode(order(id)))

  private def unreadable(id: String): Message.Incoming =
    message(id, Chunk(9.toByte))

  private def claim(of: Message.Incoming*): Dequeued =
    Dequeued.Claimed(
      Claim(Receipt("r1"), NonEmptyChunk.fromIterable(of.head, of.tail), Instant.EPOCH, lease, backlogDepth = 0)
    )

  /** A client that hands out one scripted claim and records what it was told. */
  final private class Fake(
    answers: Ref[Chunk[Dequeued]],
    val settled: Ref[Chunk[Verdict]],
    val asked: Ref[Chunk[Int]],
  ) extends QueueClient:

    override def enqueue(queue: String, message: Message.Outgoing): IO[ServiceError, Enqueued] =
      ZIO.succeed(Enqueued(0L))

    override def dequeue(queue: String, maxWait: Duration, maxBatch: Int): IO[ServiceError, Dequeued] =
      asked.update(_ :+ maxBatch) *> answers.modify {
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
      ZIO.succeed(Renewed(Chunk.empty, Instant.EPOCH, lease))

  private def fake(scripted: Dequeued*) =
    for
      answers <- Ref.make(Chunk.fromIterable(scripted))
      settled <- Ref.make(Chunk.empty[Verdict])
      asked   <- Ref.make(Chunk.empty[Int])
    yield Fake(answers, settled, asked)

  private def batchOver(
    client: QueueClient,
    size: Int = 3,
    policy: Provider.DecodingPolicy = Provider.DecodingPolicy.Retry,
  ) =
    Provider(client).batched[Order](
      Provider.BatchConsumerConfig("orders", size, patience = 1.second, heartbeat = 10.millis, policy = policy)
    )

  private def verdicts(of: Chunk[Verdict]): Map[String, Verdict.Outcome] =
    of.map(verdict => (verdict.id: String) -> verdict.outcome).toMap

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ManagedBatch")(
    test("a batch the logic answered is settled done, every message of it") {
      for
        client <- fake(claim(readable("m1"), readable("m2"), readable("m3")))
        batch  <- batchOver(client)
        worked <- Ref.make(List.empty[Order])
        _      <- batch.consume(values => worked.set(values))
        seen   <- worked.get
        given_ <- client.settled.get
      yield assertTrue(
        seen.map(_.id) == List("m1", "m2", "m3"),
        verdicts(given_).values.toSet == Set(Verdict.Outcome.Done),
        given_.size == 3,
      )
    },
    test("a batch the logic failed comes back whole, including what it had already worked") {
      for
        client  <- fake(claim(readable("m1"), readable("m2")))
        batch   <- batchOver(client)
        outcome <- batch.consume(_ => ZIO.fail(ServiceError.Rejected("mine"))).either
        given_  <- client.settled.get
      yield assertTrue(
        outcome == Left(ServiceError.Rejected("mine")),
        verdicts(given_).values.toSet == Set(Verdict.Outcome.Failed),
        given_.size == 2,
      )
    },
    test("one message nobody can read keeps the whole batch from the logic") {
      for
        client <- fake(claim(readable("m1"), unreadable("m2"), readable("m3")))
        batch  <- batchOver(client, policy = Provider.DecodingPolicy.Retry)
        ran    <- Ref.make(false)
        _      <- batch.consume(_ => ran.set(true))
        wentIn <- ran.get
        given_ <- client.settled.get
      yield assertTrue(
        !wentIn,
        verdicts(given_).values.toSet == Set(Verdict.Outcome.Failed),
        given_.size == 3,
      )
    },
    test("only the message that would not read takes the policy; its readable neighbours come back") {
      for
        client <- fake(claim(readable("m1"), unreadable("m2")))
        batch  <- batchOver(client, policy = Provider.DecodingPolicy.Discard)
        _      <- batch.consume(_ => ZIO.unit)
        given_ <- client.settled.get
      yield assertTrue(
        verdicts(given_) == Map("m1" -> Verdict.Outcome.Failed, "m2" -> Verdict.Outcome.Done)
      )
    },
    test("the policy reads each message's own attempts, so a batch may part on the count") {
      for
        client <- fake(claim(message("m1", Chunk(9.toByte), attempt = 1), message("m2", Chunk(9.toByte), attempt = 3)))
        batch  <- batchOver(client, policy = Provider.DecodingPolicy.DiscardAfter(3))
        _      <- batch.consume(_ => ZIO.unit)
        given_ <- client.settled.get
      yield assertTrue(
        verdicts(given_) == Map("m1" -> Verdict.Outcome.Failed, "m2" -> Verdict.Outcome.Done)
      )
    },
    test("the claim is asked for as many messages as the batch takes, and one takes one") {
      for
        many    <- fake(Dequeued.Idle)
        batch   <- batchOver(many, size = 7)
        _       <- batch.consume(_ => ZIO.unit)
        batched <- many.asked.get
        one     <- fake(Dequeued.Idle)
        single  <- Provider(one).consumer[Order](Provider.ConsumerConfig("orders", patience = 1.second))
        _       <- single.consume(_ => ZIO.unit)
        alone   <- one.asked.get
      yield assertTrue(batched == Chunk(7), alone == Chunk(1))
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)
