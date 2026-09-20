package homelab.keyedqueue.client.queue


import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.model.*
import zio.*
import zio.schema.{ DeriveSchema, Schema }
import zio.test.*

import java.time.Instant


/**
 * What a batch promises beyond one message at a time: every message of a claim shares an outcome.
 *
 * The cases worth testing are the ones where the outcome and the messages disagree — a batch the logic
 * failed halfway through, and a batch carrying a message the caller's own reading will not take.
 */
object ManagedBatchSpec extends ZIOSpecDefault:

  final case class Order(id: String)

  private given Schema[Order] = DeriveSchema.gen[Order]

  import MessageDecoder.auto.given

  private val lease   = 40.millis
  private val encoder = MessageEncoder.derive[Order]

  private def order(id: String): Order = Order(id)

  private def message(id: String, payload: Chunk[Byte], attempt: Int = 1): Message.Incoming =
    Message.Incoming(MessageKey("k1"), MessageId(id), "order.v1", encoder.encoding, payload, Instant.EPOCH, attempt)

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

  private def batchOver(client: QueueClient, size: Int = 3) =
    Provider(client).batched[Order](
      Provider.BatchConsumerConfig("orders", size, patience = 1.second, heartbeat = 10.millis)
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
    test("one message nobody can read fails the batch, and every message of it comes back") {
      // Reading is the caller's now, so a message that will not read fails the reading — which is a
      // handler failure like any other, and settles the way one does.
      for
        client  <- fake(claim(readable("m1"), unreadable("m2"), readable("m3")))
        batch   <- batchOver(client)
        ran     <- Ref.make(false)
        outcome <- batch.consume(_ => ran.set(true)).either
        wentIn  <- ran.get
        given_  <- client.settled.get
      yield assertTrue(
        !wentIn,
        outcome.left.exists(_.isInstanceOf[ServiceError.Unreadable]),
        verdicts(given_).values.toSet == Set(Verdict.Outcome.Failed),
        given_.size == 3,
      )
    },
    test("a caller reading the messages itself decides what an unreadable one costs") {
      // What the policy used to do, composed instead: drop the batch that will not read, by answering
      // for it. Everything else a caller might want is the same kind of edit.
      for
        client <- fake(claim(unreadable("m1")))
        raw    <- Provider(client).batchedMessages(
                    Provider.BatchConsumerConfig("orders", size = 3, patience = 1.second, heartbeat = 10.millis)
                  )
        _      <- raw.consume(messages => ZIO.foreachDiscard(messages)(readingOrDropping))
        given_ <- client.settled.get
      yield assertTrue(verdicts(given_).values.toSet == Set(Verdict.Outcome.Done))
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

  /** A caller's own reading, which answers for a message it cannot take rather than failing. */
  private def readingOrDropping(message: Message.Incoming): UIO[Unit] =
    ZIO.succeed(MessageDecoder[Order].decode(message)).unit
