package homelab.keyedqueue.client.queue.managed


import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.{ Provider, QueueClient }
import homelab.keyedqueue.client.queue.model.*
import zio.*
import zio.test.*

import java.time.Instant


/**
 * A consumer interrupted around the moment its claim arrives. A claim that has reached the consumer must be
 * settled, so it comes back at once rather than when its lease runs out.
 *
 * The only claim that can still be lost is one discarded in the instant its call returns, which no consumer
 * code can see. Over many rounds that is rare enough to bound: without holding the claim from the moment it
 * returns, about one round in four hundred loses its claim.
 */
object ManagedBatchInterruptionSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ManagedBatch under interruption")(
    test("a claim that reached the consumer is settled, whenever the interrupt lands") {
      for
        rounds  <- ZIO.foreach(Chunk.fromIterable(1 to 20000))(_ => round)
        handed   = rounds.flatten
        stranded = handed.count(settled => !settled)
      yield assertTrue(handed.size > 10000, stranded <= 10)
    }
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)

  private object Support:

    /** The one message every round's claim holds. */
    val incoming: Message.Incoming =
      Message.Incoming(MessageKey("k1"), MessageId("m1"), "order.v1", "json", Chunk.empty, Instant.EPOCH, 1)

    /** The claim every round hands out. */
    val claimed: Dequeued =
      Dequeued.Claimed(Claim(Receipt("r1"), NonEmptyChunk(incoming), Instant.EPOCH, 1.second, backlogDepth = 0))

    /**
     * One consumer, interrupted after a random number of yields, and whether the claim it was handed was
     * settled.
     *
     * @return `None` when the claim was never handed out, else whether it was settled
     */
    def round: UIO[Option[Boolean]] =
      ZIO.scoped:
        for
          client   <- Scripted.make(claimed)
          batch    <- Provider(client).batchedMessages(Provider.BatchConsumerConfig("orders", 1, patience = 1.second))
          consumer <- batch.consume(_ => ZIO.never).fork
          _        <- jitter
          _        <- consumer.interrupt
          handed   <- client.handedOut
          settled  <- client.settled
        yield Option.when(handed)(settled)

    /**
     * A random pause, in yields rather than time, so the moments it lands on vary without slowing a round.
     *
     * @return noop after up to 40 yields
     */
    def jitter: UIO[Unit] = Random.nextIntBounded(40).flatMap(yields => ZIO.yieldNow.repeatN(yields)).unit

    /**
     * A client that answers one scripted dequeue at once and records whether anything was settled.
     *
     * @param answers what is still to be handed out
     * @param settles how many settles it was asked for
     */
    final class Scripted(answers: Ref[Chunk[Dequeued]], settles: Ref[Int]) extends QueueClient:

      /**
       * Whether the scripted answer has been handed out.
       *
       * @return true once a dequeue has taken it
       */
      def handedOut: UIO[Boolean] = answers.get.map(_.isEmpty)

      /**
       * Whether anything was settled.
       *
       * @return true once a settle arrived
       */
      def settled: UIO[Boolean] = settles.get.map(_ > 0)

      override def enqueue(queue: String, message: Message.Outgoing): IO[ServiceError, Enqueued] =
        ZIO.succeed(Enqueued(0L))

      override def dequeue(queue: String, maxWait: Duration, maxBatch: Int): IO[ServiceError, Dequeued] =
        answers.modify:
          case answer +: rest => (answer, rest)
          case _              => (Dequeued.Idle, Chunk.empty)

      override def settle(receipt: Receipt, verdicts: Chunk[Verdict], retryAfter: Duration): IO[ServiceError, Settled] =
        settles.update(_ + 1).as(Settled.Applied)

      override def heartbeat(receipts: Chunk[Receipt]): IO[ServiceError, Renewed] =
        ZIO.succeed(Renewed(Chunk.empty, Instant.EPOCH, 1.second))

    object Scripted:

      /**
       * A client with one answer to hand out.
       *
       * @param answer what the first dequeue gets
       * @return the client
       */
      def make(answer: Dequeued): UIO[Scripted] =
        for
          answers <- Ref.make(Chunk(answer))
          settles <- Ref.make(0)
        yield Scripted(answers, settles)
