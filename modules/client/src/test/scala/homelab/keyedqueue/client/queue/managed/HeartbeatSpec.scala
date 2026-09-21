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

  /** Ten cadences, so a window counts beats rather than catching one. */
  private val window = lease * 5

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
      // Counted against what one beat manages in the same run, so a runner that stalls lowers both
      // windows: only a second fiber raises the later one. The lower bound is what makes the comparison
      // mean anything, and it is also what pins the cadence to the lease the claim was granted — a beat
      // keeping to a span of its own puts nothing in either window.
      for
        client    <- counting
        heartbeat <- Heartbeat.make(client)
        _         <- heartbeat.hold(claim("r1", "m1"))
        _         <- client.beats.take
        _         <- client.beats.takeAll
        _         <- ZIO.sleep(window)
        alone     <- client.beats.size
        _         <- client.beats.takeAll
        _         <- heartbeat.release(Receipt("r1")) *> heartbeat.hold(claim("r2", "m2"))
        _         <- ZIO.sleep(window)
        after     <- client.beats.size
      yield assertTrue(alone >= 2, after <= alone + 2)
    }
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)
