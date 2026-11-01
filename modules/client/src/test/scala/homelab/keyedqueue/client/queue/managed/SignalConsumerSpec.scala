package homelab.keyedqueue.client.queue.managed


import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.{ MessageEncoder, Provider, QueueClient }
import homelab.keyedqueue.client.queue.model.*
import zio.*
import zio.test.*

import java.time.Instant


/**
 * What a signal consumer promises over a batch: a claim's signals name one key however many arrived, so the
 * logic runs once for it, and the claim settles as a whole on what that answers.
 */
object SignalConsumerSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Provider.signalConsumer")(
    test("a claim of repeated signals for one key runs the logic once, with that key, and settles every one done") {
      ZIO.scoped:
        for
          client   <- fake(claim(signal("s1"), signal("s2"), signal("s3")))
          consumer <- Provider(client).signalConsumer(config(retryAfter = Duration.Zero))
          seen     <- Ref.make(Chunk.empty[Ready])
          _        <- consumer.consume(ready => seen.update(_ :+ ready))
          got      <- seen.get
          settled  <- client.settled.get
        yield assertTrue(
          got == Chunk(Ready("k1")),
          settled.map(_.outcome).toSet == Set(Verdict.Outcome.Done),
          settled.map(_.id: String).toSet == Set("s1", "s2", "s3"),
        )
    },
    test("a failing logic settles the whole claim failed, so every signal comes back after retryAfter") {
      ZIO.scoped:
        for
          client   <- fake(claim(signal("s1"), signal("s2")))
          consumer <- Provider(client).signalConsumer(config(retryAfter = 2.seconds))
          exit     <- consumer.consume(_ => ZIO.fail(ServiceError.Rejected("not ready"))).exit
          settled  <- client.settled.get
          waits    <- client.waits.get
        yield assertTrue(
          exit.isFailure,
          settled.map(_.outcome).toSet == Set(Verdict.Outcome.Failed),
          settled.map(_.id: String).toSet == Set("s1", "s2"),
          waits == Chunk(2.seconds),
        )
    },
  )

  private object Support:

    /**
     * A signal for key `k1`: what a signal consumer reads is the key, so the payload is empty.
     *
     * @param id the message id
     * @return the delivery
     */
    def signal(id: String): Message.Incoming =
      Message.Incoming(MessageKey("k1"), MessageId(id), MessageEncoder.unnamed, MessageEncoder.protobuf, Chunk.empty, Instant.EPOCH, attempt = 1)

    /**
     * One claim over `of`, all on key `k1`.
     *
     * @param of the claim's messages, at least one
     * @return the dequeue answer handing them out
     */
    def claim(of: Message.Incoming*): Dequeued =
      Dequeued.Claimed(Claim(Receipt("r1"), NonEmptyChunk.fromIterable(of.head, of.tail), Instant.EPOCH, 30.seconds, backlogDepth = 0))

    /**
     * A signal consumer's configuration over the `signals` queue.
     *
     * @param retryAfter how long a failed claim waits before it is delivered again
     * @return the configuration
     */
    def config(retryAfter: Duration): Provider.BatchConsumerConfig =
      Provider.BatchConsumerConfig("signals", 10, patience = 1.second, retryAfter = retryAfter)

    /**
     * A client that hands out one scripted claim, then nothing, and records how it was settled.
     *
     * @param answers what each dequeue answers, in order
     * @param settled every verdict it was told
     * @param waits every `retryAfter` it was told
     */
    final class Fake(answers: Ref[Chunk[Dequeued]], val settled: Ref[Chunk[Verdict]], val waits: Ref[Chunk[Duration]]) extends QueueClient:

      override def enqueue(queue: String, message: Message.Outgoing): IO[ServiceError, Enqueued] =
        ZIO.succeed(Enqueued(0L))

      override def dequeue(queue: String, maxWait: Duration, maxBatch: Int): IO[ServiceError, Dequeued] =
        answers.modify:
          case answer +: rest => (answer, rest)
          case _              => (Dequeued.Idle, Chunk.empty)

      override def settle(receipt: Receipt, verdicts: Chunk[Verdict], retryAfter: Duration): IO[ServiceError, Settled] =
        settled.update(_ ++ verdicts) *> waits.update(_ :+ retryAfter).as(Settled.Applied)

      override def heartbeat(receipts: Chunk[Receipt]): IO[ServiceError, Renewed] =
        ZIO.succeed(Renewed(Chunk.empty, Instant.EPOCH, 30.seconds))

    /**
     * A client scripted with `scripted`.
     *
     * @param scripted what its dequeues answer, in order
     * @return the client
     */
    def fake(scripted: Dequeued*): UIO[Fake] =
      for
        answers <- Ref.make(Chunk.fromIterable(scripted))
        settled <- Ref.make(Chunk.empty[Verdict])
        waits   <- Ref.make(Chunk.empty[Duration])
      yield Fake(answers, settled, waits)
