package homelab.keyedqueue.client.queue.managed


import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.QueueClient
import homelab.keyedqueue.client.queue.model.{ Claim, Dequeued, Enqueued, Message, MessageId, MessageKey, Receipt, Renewed, Settled, Verdict }
import zio.*
import zio.test.*

import java.time.Instant


/**
 * What the beat promises about its own fiber: one runs while claims are held, and a claim taken as the
 * last one is settled joins it rather than starting a second.
 */
object HeartbeatSpec extends ZIOSpecDefault:

  private val lease = 20.millis

  /** Ten cadences, so the window counts beats rather than catching one. */
  private val window = lease * 5

  /** The most one beat sends in the window: a cadence is half the lease, and one falls on its edge. */
  private val alone = (window.toMillis / (lease.toMillis / 2) + 1).toInt

  /** Long enough that a loaded runner is not the thing under test, short enough to fail a beat keeping to
    * a span of its own rather than to the lease it was granted. */
  private val soon = lease * 25

  private def message(id: String): Message.Incoming =
    Message.Incoming(MessageKey("k1"), MessageId(id), "order.v1", "application/json", Chunk.empty, Instant.EPOCH, 1)

  private def claim(receipt: String, of: String): Claim =
    Claim(Receipt(receipt), NonEmptyChunk(message(of)), Instant.EPOCH, lease, backlogDepth = 0)

  /** A client that records every beat and answers each on the same lease. */
  final private class Counting(val beats: Queue[Chunk[Receipt]]) extends QueueClient:

    override def enqueue(queue: String, message: Message.Outgoing): IO[ServiceError, Enqueued] =
      ZIO.succeed(Enqueued(0L))

    override def dequeue(queue: String, maxWait: Duration, maxBatch: Int): IO[ServiceError, Dequeued] =
      ZIO.succeed(Dequeued.Idle)

    override def settle(
      receipt: Receipt,
      verdicts: Chunk[Verdict],
      retryAfter: Duration,
    ): IO[ServiceError, Settled] =
      ZIO.succeed(Settled.Applied)

    override def heartbeat(receipts: Chunk[Receipt]): IO[ServiceError, Renewed] =
      beats.offer(receipts).as(Renewed(Chunk.empty, Instant.EPOCH, lease))

  private def counting: UIO[Counting] =
    Queue.unbounded[Chunk[Receipt]].map(Counting(_))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Heartbeat")(
    test("a claim taken as the last one is settled joins the beat rather than starting a second") {
      // Counted against what a single beat can send, which is a ceiling a slow runner only falls under:
      // sleeping a cadence between beats is what bounds it, so the count rises above this only when
      // there is more than one beat sending. Taking the first bounds the cadence, which the ceiling
      // assumes and cannot itself check.
      for
        client    <- counting
        heartbeat <- Heartbeat.make(client)
        _         <- heartbeat.hold(claim("r1", "m1"))
        first     <- client.beats.take.timeout(soon)
        _         <- heartbeat.release(Receipt("r1")) *> heartbeat.hold(claim("r2", "m2"))
        _         <- client.beats.takeAll
        _         <- ZIO.sleep(window)
        sent      <- client.beats.size
      yield assertTrue(first.isDefined, sent <= alone + 2)
    }
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)
