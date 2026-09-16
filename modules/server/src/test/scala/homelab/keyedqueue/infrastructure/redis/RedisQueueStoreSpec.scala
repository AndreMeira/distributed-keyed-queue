package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.SpecHelper
import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.domain.model.{ Claim, Demand, Grant, Message, Settlement, Submission }
import homelab.keyedqueue.domain.model.Message.Encoding
import homelab.keyedqueue.domain.model.Settlement.Verdict
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.keys.KeyLayout
import zio.*
import zio.test.*


/**
 * The guarantees the whole design exists for, against a real substrate.
 *
 * These are the conformance tests the roadmap asks for: per-key order, one worker per key, nothing lost when
 * a holder dies, and a failure that retries rather than disappears. They talk to the port, not to Redis, so
 * a second substrate has to pass them unchanged.
 */
object RedisQueueStoreSpec extends ZIOSpecDefault:

  private val leaseTtl = 2.seconds

  /** The layout these tests read and write under. */
  private val layout: KeyLayout = KeyLayout.single

  def spec: Spec[TestEnvironment & Scope, Any] = {
    suite("QueueStore over Redis")(
      test("attemptClaim answers at once, whatever patience the demand carries") {
        // The non-waiting half of the port: 30 seconds of patience buys nothing here, because waiting is
        // the readiness's job and an attempt only reports what was true when it asked.
        for
          worker          <- ZIO.service[QueueStore]
          queue            = QueueName("attempt")
          outcome         <- worker.attemptClaim(Demand(queue, 30.seconds, 1)).timed
          (elapsed, found) = outcome
        yield assertTrue(found.isEmpty, elapsed < 1.second)
      },
      test("a key's messages are delivered oldest first") {
        for
          worker <- ZIO.service[QueueStore]
          queue   = QueueName("order")
          key     = MessageKey("k1")
          _      <- ZIO.foreachDiscard(List("a", "b", "c"))(m => worker.enqueue(Submission(queue, Helper.message(key, m))))
          seen   <- ZIO.foreach(1 to 3)(_ => Helper.one(worker, queue).flatMap(Helper.ack(worker, queue)))
        yield assertTrue(seen == Chunk("a", "b", "c"))
      },
      test("keys are served in the order they became claimable, not just messages within a key") {
        // Cross-key FIFO. Nothing asserted this before, and it is the property the `ready` structure carries:
        // whatever holds claimable keys has to hand them out oldest-first, whether that is a list's head or a
        // sorted set's lowest score. A change to that structure that got the ordering wrong would otherwise
        // be invisible — the ordering tests above are all *within* one key.
        for
          worker <- ZIO.service[QueueStore]
          queue   = QueueName("cross-key-order")
          // Named against the alphabet on purpose: several keys can become claimable inside one millisecond,
          // and a tie broken by member name rather than by arrival would pass with ascending names and be
          // wrong. These fail unless the order really is the order they arrived in.
          keys    = Chunk("k5", "k4", "k3", "k2", "k1").map(MessageKey.apply)
          _      <- ZIO.foreachDiscard(keys)(key => worker.enqueue(Submission(queue, Helper.message(key, s"m-$key"))))
          // A second message for the first key, after the rest: it must not move that key's place, and must
          // not give it a second one either.
          _      <- worker.enqueue(Submission(queue, Helper.message(keys.head, "m-again")))
          served <- ZIO.foreach(1 to keys.size): _ =>
                      Helper
                        .one(worker, queue)
                        .flatMap: batch =>
                          val held = batch.get
                          worker.settle(Helper.settlement(held.claim, Helper.acks(held))).as(held.claim.key)
        yield assertTrue(Chunk.fromIterable(served) == keys)
      },
      test("a key being worked is not handed to anybody else, and its next message waits") {
        // The invariant the whole design is built around. While k1 is held, a second claim must find k2 —
        // never k1's next message, and never k1 again.
        for
          worker     <- ZIO.service[QueueStore]
          queue       = QueueName("exclusive")
          _          <- worker.enqueue(Submission(queue, Helper.message(MessageKey("k1"), "first")))
          _          <- worker.enqueue(Submission(queue, Helper.message(MessageKey("k1"), "second")))
          _          <- worker.enqueue(Submission(queue, Helper.message(MessageKey("k2"), "other-key")))
          held       <- worker.attemptClaim(Demand(queue, 2.seconds, 1))
          while_held <- worker.attemptClaim(Demand(queue, 2.seconds, 1))
        yield assertTrue(
          held.map(Helper.body) == Some(Chunk("first")),
          while_held.map(Helper.body) == Some(Chunk("other-key")), // a different key, never k1's queued second
        )
      },
      test("a failure leaves the message where it was, and counts the attempt") {
        for
          worker <- ZIO.service[QueueStore]
          queue   = QueueName("retry")
          key     = MessageKey("k1")
          _      <- worker.enqueue(Submission(queue, Helper.message(key, "poison")))
          _      <- worker.enqueue(Submission(queue, Helper.message(key, "after")))
          first  <- worker.attemptClaim(Demand(queue, 2.seconds, 1))
          _      <- ZIO.foreachDiscard(first)(Helper.nack(worker))
          second <- worker.attemptClaim(Demand(queue, 2.seconds, 1))
        yield assertTrue(
          second.map(Helper.body) == Some(Chunk("poison")), // the same message, not the one behind it
          second.flatMap(_.messages.headOption.map(_.attempt)) == Some(2),
        )
      },
      test("a settle after the lease lapsed is rejected, and the messages come back") {
        for
          worker <- ZIO.service[QueueStore]
          queue   = QueueName("lapsed")
          key     = MessageKey("k1")
          _      <- worker.enqueue(Submission(queue, Helper.message(key, "work")))
          held   <- worker.attemptClaim(Demand(queue, 2.seconds, 1))
          _      <- ZIO.sleep(leaseTtl + 500.millis)
          _      <- worker.sweep(queue, 100)
          late   <- ZIO.foreach(held)(batch => worker.settle(Helper.settlement(batch.claim, Helper.acks(batch))))
          again  <- worker.attemptClaim(Demand(queue, 2.seconds, 1))
        yield assertTrue(
          late == Some(false),                          // the token was spent by the revoke
          again.map(Helper.body) == Some(Chunk("work")), // and the work came back
        )
      },
      test("a claim hands over a batch, in producer order, with what is left behind counted") {
        for
          worker <- ZIO.service[QueueStore]
          queue   = QueueName("batch")
          key     = MessageKey("k1")
          _      <- ZIO.foreachDiscard(List("a", "b", "c", "d"))(m => worker.enqueue(Submission(queue, Helper.message(key, m))))
          held   <- worker.attemptClaim(Demand(queue, 2.seconds, 3))
        yield assertTrue(
          held.map(Helper.body).contains(Chunk("a", "b", "c")),
          held.map(_.backlogDepth).contains(1), // d, still queued
          held.map(_.messages.map(_.attempt).toChunk).contains(Chunk(1, 1, 1)),
        )
      },
      test("a batch larger than the key holds returns what there is") {
        for
          worker <- ZIO.service[QueueStore]
          queue   = QueueName("batch-short")
          key     = MessageKey("k1")
          _      <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(Submission(queue, Helper.message(key, m))))
          held   <- worker.attemptClaim(Demand(queue, 2.seconds, 10))
        yield assertTrue(
          held.map(Helper.body).contains(Chunk("a", "b")),
          held.map(_.backlogDepth).contains(0),
        )
      },
      test("a claim settled piece by piece keeps its key until nothing is owed") {
        for
          worker  <- ZIO.service[QueueStore]
          queue    = QueueName("partial")
          key      = MessageKey("k1")
          _       <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(Submission(queue, Helper.message(key, m))))
          held    <- worker.attemptClaim(Demand(queue, 2.seconds, 2))
          batch    = held.get
          first   <- worker.settle(Helper.settlement(batch.claim, NonEmptyChunk(batch.messages(0).id -> Verdict.Done)))
          // Still owed the second, so the key is nobody else's yet.
          blocked <- worker.attemptClaim(Demand(queue, 1.second, 1))
          second  <- worker.settle(Helper.settlement(batch.claim, NonEmptyChunk(batch.messages(1).id -> Verdict.Done)))
          after   <- worker.attemptClaim(Demand(queue, 1.second, 1))
        yield assertTrue(first, second, blocked.isEmpty, after.isEmpty) // nothing left, so nothing to claim
      },
      test("a nacked message stays in its place, and the key comes back when nothing is owed") {
        // Producer order survives a settle in any order: 1 acked, 2 nacked, 4 acked, 3 nacked leaves 2 and 3
        // where the producer put them.
        for
          worker <- ZIO.service[QueueStore]
          queue   = QueueName("mixed")
          key     = MessageKey("k1")
          _      <- ZIO.foreachDiscard(List("1", "2", "3", "4", "5"))(m => worker.enqueue(Submission(queue, Helper.message(key, m))))
          held   <- worker.attemptClaim(Demand(queue, 2.seconds, 4))
          batch   = held.get
          by      = batch.messages.map(owned => Helper.cargo(owned.message) -> owned.id).toMap
          _      <- worker.settle(Helper.settlement(batch.claim, NonEmptyChunk(by("1") -> Verdict.Done)))
          _      <- worker.settle(Helper.settlement(batch.claim, NonEmptyChunk(by("2") -> Verdict.Failed)))
          _      <- worker.settle(Helper.settlement(batch.claim, NonEmptyChunk(by("4") -> Verdict.Done)))
          _      <- worker.settle(Helper.settlement(batch.claim, NonEmptyChunk(by("3") -> Verdict.Failed)))
          again  <- worker.attemptClaim(Demand(queue, 2.seconds, 5))
        yield assertTrue(
          again.map(Helper.body).contains(Chunk("2", "3", "5")),                // producer order, whatever order they were settled in
          again.map(_.messages.map(_.attempt).toChunk).contains(Chunk(2, 2, 1)), // the nacked ones carry their count
        )
      },
      test("a settle naming a message the claim does not own changes nothing") {
        for
          worker  <- ZIO.service[QueueStore]
          queue    = QueueName("unowned")
          key      = MessageKey("k1")
          _       <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(Submission(queue, Helper.message(key, m))))
          held    <- worker.attemptClaim(Demand(queue, 2.seconds, 1))
          batch    = held.get
          // "b" is queued but not owned by this claim, and "nowhere" is nobody's.
          applied <- worker.settle(
                       Helper.settlement(
                         batch.claim,
                         NonEmptyChunk(MessageId("b") -> Verdict.Done, MessageId("nowhere") -> Verdict.Done),
                       )
                     )
          // Still owed "a", so the claim is alive and the key is still held.
          _       <- worker.settle(Helper.settlement(batch.claim, Helper.acks(batch)))
          after   <- worker.attemptClaim(Demand(queue, 2.seconds, 2))
        yield assertTrue(applied, after.map(Helper.body).contains(Chunk("b")))
      },
      test("the same id enqueued twice for a key is one message") {
        // HSETNX in enqueue.lua: a producer retrying an at-least-once send must not double the work.
        for
          worker <- ZIO.service[QueueStore]
          queue   = QueueName("idempotent")
          key     = MessageKey("k1")
          first  <- worker.enqueue(Submission(queue, Helper.message(key, "once")))
          again  <- worker.enqueue(Submission(queue, Helper.message(key, "once")))
          held   <- worker.attemptClaim(Demand(queue, 2.seconds, 5))
        yield assertTrue(first == 1L, again == 1L, held.map(Helper.body).contains(Chunk("once")))
      },
      test("acknowledging clears the payload as well as the place in line") {
        for
          worker   <- ZIO.service[QueueStore]
          redis    <- ZIO.serviceWithZIO[Connection](_.provide(ZIO.service[Connection.Commands]))
          queue     = QueueName("cleanup")
          key       = MessageKey("k1")
          _        <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(Submission(queue, Helper.message(key, m))))
          held     <- worker.attemptClaim(Demand(queue, 2.seconds, 2))
          _        <- ZIO.foreachDiscard(held)(batch => worker.settle(Helper.settlement(batch.claim, Helper.acks(batch))))
          payloads <- ZIO.attemptBlocking(redis.hlen(layout.queue(QueueName("cleanup")).payloads(MessageKey("k1")))).orDie
          owned    <- ZIO.attemptBlocking(redis.scard(layout.queue(QueueName("cleanup")).owned(MessageKey("k1")))).orDie
          // Idle is the absence of the key from every structure — there is no state entry to check any more.
          claimed  <- ZIO.attemptBlocking(redis.zcard(layout.queue(QueueName("cleanup")).claimed)).orDie
          ready    <- ZIO.attemptBlocking(redis.zcard(layout.queue(QueueName("cleanup")).ready)).orDie
        yield assertTrue(payloads == 0L, owned == 0L, claimed == 0L, ready == 0L)
      },
      test("a claim reclaimed while a nack's backoff is pending queues its key once, not twice") {
        // A partial nack can set a backoff and leave the claim alive. If that claim then lapses, the reclaim
        // and the due-sweep would each push the key onto `ready` — two entries, two claimers, and one of them
        // working for nothing. The fence would stop it corrupting anything; it would still be waste.
        for
          worker <- ZIO.service[QueueStore]
          redis  <- ZIO.serviceWithZIO[Connection](_.provide(ZIO.service[Connection.Commands]))
          queue   = QueueName("backoff-reclaim")
          key     = MessageKey("k1")
          _      <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(Submission(queue, Helper.message(key, m))))
          held   <- worker.attemptClaim(Demand(queue, 2.seconds, 2))
          batch   = held.get
          // Nack the first with a backoff; the second stays owed, so the claim lives on.
          _      <- worker.settle(Helper.settlement(batch.claim, NonEmptyChunk(batch.messages(0).id -> Verdict.Failed), 1.second))
          // Let the lease lapse and the backoff fall due, then sweep both in one pass.
          _      <- ZIO.sleep(leaseTtl + 1.second)
          _      <- worker.sweep(queue, 100)
          ready  <- ZIO.attemptBlocking(redis.zcard(layout.queue(queue).ready)).orDie
        yield assertTrue(ready == 1L)
      },
      test("a sweep names what it reclaimed and what it released") {
        // The one port method whose answer nothing else asserts. Two keys, repaired for different reasons:
        // k1's holder went silent and its lease lapsed, k2 was nacked with a backoff that has since fallen
        // due. Both come back claimable, and the sweep says which was which.
        for
          worker  <- ZIO.service[QueueStore]
          queue    = QueueName("swept")
          _       <- worker.enqueue(Submission(queue, Helper.message(MessageKey("k1"), "abandoned")))
          _       <- worker.enqueue(Submission(queue, Helper.message(MessageKey("k2"), "backed-off")))
          held    <- worker.attemptClaim(Demand(queue, 2.seconds, 1))
          second  <- worker.attemptClaim(Demand(queue, 2.seconds, 1))
          // k2 is nacked with a backoff; k1 is simply dropped, so only its lease can return it.
          _       <- ZIO.foreachDiscard(second)(batch =>
                       worker.settle(Helper.settlement(batch.claim, Helper.acks(batch).map((id, _) => id -> Verdict.Failed), 300.millis))
                     )
          _       <- ZIO.sleep(leaseTtl + 500.millis)
          swept   <- worker.sweep(queue, 100)
          claimed <- worker.attemptClaim(Demand(queue, 2.seconds, 1))
        yield assertTrue(
          swept.reclaimed.map(_.toString) == Chunk("k1"),
          swept.released.map(_.toString) == Chunk("k2"),
          claimed.isDefined, // both are claimable again, whichever comes first
        )
      },
      test("a heartbeat renews what is held and names what is lost") {
        for
          worker        <- ZIO.service[QueueStore]
          queue          = QueueName("beat")
          _             <- worker.enqueue(Submission(queue, Helper.message(MessageKey("k1"), "work")))
          held          <- worker.attemptClaim(Demand(queue, 2.seconds, 1))
          ghost          = Claim(queue, MessageKey("gone"), Token(7))
          renewed       <- worker.renew(Chunk.fromIterable(held.map(_.claim)) :+ ghost)
          (until, stale) = renewed
        yield assertTrue(
          stale.map(_.key.toString) == Chunk("gone"), // only the one that was never held
          until.toEpochMilli > 0L,
        )
      },
    ) @@ RedisSpecSupport.Aspect.init @@ SpecHelper.Aspect.common
  }.provideSomeShared[Scope](RedisSpecSupport.config(leaseTtl) >+> RedisSpecSupport.layer)
