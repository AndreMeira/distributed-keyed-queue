package homelab.keyedqueue


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ Claim, Claimed, Demand, Message, Settlement, Submission }
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis.{ Connection, Namespace, RedisQueueStore, Scripts, WakeListener, Waiters }
import io.lettuce.core.cluster.api.sync.RedisClusterCommands
import org.testcontainers.containers.GenericContainer
import zio.*
import zio.test.*


/**
 * The guarantees the whole design exists for, against a real substrate.
 *
 * These are the conformance tests the roadmap asks for: per-key order, one worker per key, nothing lost when
 * a holder dies, and a failure that retries rather than disappears. They talk to the port, not to Redis, so
 * a second substrate has to pass them unchanged.
 */
object QueueStoreSpec extends ZIOSpecDefault:

  private val leaseTtl = 2.seconds

  /** These tests run one bucket, so the tag every key carries is this one's. */
  private val buckets = 1

  /** A Valkey container for the suite, and two stores over it — a worker's, and a sweeper's. */
  private val substrate: ZLayer[Any, Any, (QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])] =
    ZLayer.scoped:
      for
        container       <- ZIO.acquireRelease(
                             ZIO.attemptBlocking:
                               // Docker Engine 29 rejects the API version docker-java negotiates by default with
                               // an HTTP 400; pinning it is what the toolkit's Testcontainers specs do too.
                               val _                            = java.lang.System.setProperty("api.version", "1.40")
                               // GenericContainer is self-referentially generic (SELF extends GenericContainer),
                               // which Scala infers as Nothing — hence the explicit wildcard and no chaining.
                               val started: GenericContainer[?] = GenericContainer("valkey/valkey:8.1-alpine")
                               started.setExposedPorts(java.util.List.of(Integer.valueOf(6379)))
                               started.start()
                               started
                           )(container => ZIO.attemptBlocking(container.stop()).ignore)
        url              = s"redis://${container.getHost}:${container.getMappedPort(6379)}"
        config           = QueueConfig(url, cluster = false, 0, leaseTtl, 1.second, 100, 200.millis, buckets, 5.seconds, maxBatchLimit = 32)
        (first, pooled) <- store(config)
        (second, _)     <- store(config)
        // To assert on what the adapter wrote. Borrowed from a store's own pool rather than opened here:
        // a hand-rolled connection would need its own copy of the codec, and a copy that drifted would
        // make these assertions read bytes differently from the code they are checking.
        inspect         <- pooled.provide(ZIO.service[Connection.Commands])
      yield (first: QueueStore, second: QueueStore, inspect)

  /**
   * A store with its own client and connections, as the service builds one — so the two stores here are
   * two instances, not two halves of one.
   *
   * @param config where Redis is, and the sizes to build with
   * @return the store, and the pool behind it for tests that need to look at Redis directly
   */
  private def store(config: QueueConfig): ZIO[Scope, QueueError, (QueueStore, Connection)] =
    for
      connection <- Connection.make(
                      Connection.Config(config.maxWait, config.redisUrl, config.cluster)
                    )
      scripts    <- connection.provide(Scripts.make)
      waiters    <- Waiters.make
      listener   <- WakeListener.make(connection, waiters, config.wakeBuckets, config.wakeBlock)
      _          <- listener.run.forkScoped
      store      <- RedisQueueStore.make(connection, scripts, waiters, config.leaseTtl, config.wakeBuckets)
    yield (store, connection)

  /** A message whose cargo is `body`: these tests care about order and ownership, not about content. */
  private def message(key: MessageKey, body: String): Message =
    Message(key, messageId = MessageId(body), payloadType = "test.Text", Encoding.Json, None, Chunk.fromArray(body.getBytes("UTF-8")))

  private def cargo(message: Message): String = String(message.payload.toArray, "UTF-8")

  /** What a batch is carrying, as text, in the order it was handed over. */
  private def body(batch: Claimed): Chunk[String] = batch.messages.map(owned => cargo(owned.message)).toChunk

  /** Acknowledge everything a batch owns. Non-empty, because a batch is. */
  private def acks(batch: Claimed): NonEmptyChunk[(MessageId, Verdict)] = batch.messages.map(_.id -> Verdict.Done)

  /**
   * What the use case builds before it calls the port, in the spec's own vocabulary.
   *
   * @param claim the claim being settled against
   * @param outcomes what became of each message named
   * @param retryAfter how long the key should wait; zero for "as soon as it is free"
   * @return the settlement to hand the store
   */
  private def settlement(
    claim: Claim,
    outcomes: NonEmptyChunk[(MessageId, Verdict)],
    retryAfter: Duration = Duration.Zero,
  ): Settlement =
    Settlement(
      claim,
      outcomes.map((id, verdict) => Settlement.Outcome(id, verdict)),
      Option.when(retryAfter.toMillis > 0)(retryAfter),
    )

  /** Claim exactly one message, for the tests that are not about batching. */
  private def one(store: QueueStore, queue: QueueName): ZIO[Any, QueueError, Option[Claimed]] =
    store.claim(Demand(queue, 2.seconds, 1))

  /** Acknowledge a single-message batch and report what it was carrying. */
  private def ack(store: QueueStore, queue: QueueName)(batch: Option[Claimed]): ZIO[Any, QueueError, String] =
    ZIO
      .foreach(batch)(one => store.settle(settlement(one.claim, acks(one))).as(body(one).mkString))
      .map(_.getOrElse(""))

  /** Report a whole batch as failed. */
  private def nack(store: QueueStore)(batch: Claimed): ZIO[Any, QueueError, Boolean] =
    store.settle(settlement(batch.claim, batch.messages.map(_.id -> Verdict.Failed)))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("QueueStore over Redis")(
    test("a key's messages are delivered oldest first") {
      for
        (worker, _, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue           = QueueName("order")
        key             = MessageKey("k1")
        _              <- ZIO.foreachDiscard(List("a", "b", "c"))(m => worker.enqueue(Submission(queue, message(key, m))))
        seen           <- ZIO.foreach(1 to 3)(_ => one(worker, queue).flatMap(ack(worker, queue)))
      yield assertTrue(seen == Chunk("a", "b", "c"))
    },
    test("keys are served in the order they became claimable, not just messages within a key") {
      // Cross-key FIFO. Nothing asserted this before, and it is the property the `ready` structure carries:
      // whatever holds claimable keys has to hand them out oldest-first, whether that is a list's head or a
      // sorted set's lowest score. A change to that structure that got the ordering wrong would otherwise
      // be invisible — the ordering tests above are all *within* one key.
      for
        (worker, _, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue           = QueueName("cross-key-order")
        // Named against the alphabet on purpose: several keys can become claimable inside one millisecond,
        // and a tie broken by member name rather than by arrival would pass with ascending names and be
        // wrong. These fail unless the order really is the order they arrived in.
        keys            = Chunk("k5", "k4", "k3", "k2", "k1").map(MessageKey.apply)
        _              <- ZIO.foreachDiscard(keys)(key => worker.enqueue(Submission(queue, message(key, s"m-$key"))))
        // A second message for the first key, after the rest: it must not move that key's place, and must
        // not give it a second one either.
        _              <- worker.enqueue(Submission(queue, message(keys.head, "m-again")))
        served         <- ZIO.foreach(1 to keys.size): _ =>
                            one(worker, queue).flatMap: batch =>
                              val held = batch.get
                              worker.settle(settlement(held.claim, acks(held))).as(held.claim.key)
      yield assertTrue(Chunk.fromIterable(served) == keys)
    },
    test("a key being worked is not handed to anybody else, and its next message waits") {
      // The invariant the whole design is built around. While k1 is held, a second claim must find k2 —
      // never k1's next message, and never k1 again.
      for
        (worker, other, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue               = QueueName("exclusive")
        _                  <- worker.enqueue(Submission(queue, message(MessageKey("k1"), "first")))
        _                  <- worker.enqueue(Submission(queue, message(MessageKey("k1"), "second")))
        _                  <- worker.enqueue(Submission(queue, message(MessageKey("k2"), "other-key")))
        held               <- worker.claim(Demand(queue, 2.seconds, 1))
        while_held         <- other.claim(Demand(queue, 2.seconds, 1))
      yield assertTrue(
        held.map(body) == Some(Chunk("first")),
        while_held.map(body) == Some(Chunk("other-key")), // a different key, never k1's queued second
      )
    },
    test("a failure leaves the message where it was, and counts the attempt") {
      for
        (worker, _, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue           = QueueName("retry")
        key             = MessageKey("k1")
        _              <- worker.enqueue(Submission(queue, message(key, "poison")))
        _              <- worker.enqueue(Submission(queue, message(key, "after")))
        first          <- worker.claim(Demand(queue, 2.seconds, 1))
        _              <- ZIO.foreachDiscard(first)(nack(worker))
        second         <- worker.claim(Demand(queue, 2.seconds, 1))
      yield assertTrue(
        second.map(body) == Some(Chunk("poison")), // the same message, not the one behind it
        second.flatMap(_.messages.headOption.map(_.attempt)) == Some(2),
      )
    },
    test("a settle after the lease lapsed is rejected, and the messages come back") {
      for
        (worker, sweeper, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue                 = QueueName("lapsed")
        key                   = MessageKey("k1")
        _                    <- worker.enqueue(Submission(queue, message(key, "work")))
        held                 <- worker.claim(Demand(queue, 2.seconds, 1))
        _                    <- ZIO.sleep(leaseTtl + 500.millis)
        _                    <- sweeper.sweep(queue, 100)
        late                 <- ZIO.foreach(held)(batch => worker.settle(settlement(batch.claim, acks(batch))))
        again                <- worker.claim(Demand(queue, 2.seconds, 1))
      yield assertTrue(
        late == Some(false),                   // the token was spent by the revoke
        again.map(body) == Some(Chunk("work")), // and the work came back
      )
    },
    test("a claim hands over a batch, in producer order, with what is left behind counted") {
      for
        (worker, _, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue           = QueueName("batch")
        key             = MessageKey("k1")
        _              <- ZIO.foreachDiscard(List("a", "b", "c", "d"))(m => worker.enqueue(Submission(queue, message(key, m))))
        held           <- worker.claim(Demand(queue, 2.seconds, 3))
      yield assertTrue(
        held.map(body) == Some(Chunk("a", "b", "c")),
        held.map(_.backlogDepth) == Some(1), // d, still queued
        held.map(_.messages.map(_.attempt).toChunk) == Some(Chunk(1, 1, 1)),
      )
    },
    test("a batch larger than the key holds returns what there is") {
      for
        (worker, _, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue           = QueueName("batch-short")
        key             = MessageKey("k1")
        _              <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(Submission(queue, message(key, m))))
        held           <- worker.claim(Demand(queue, 2.seconds, 10))
      yield assertTrue(held.map(body) == Some(Chunk("a", "b")), held.map(_.backlogDepth) == Some(0))
    },
    test("a claim settled piece by piece keeps its key until nothing is owed") {
      for
        (worker, other, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue               = QueueName("partial")
        key                 = MessageKey("k1")
        _                  <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(Submission(queue, message(key, m))))
        held               <- worker.claim(Demand(queue, 2.seconds, 2))
        batch               = held.get
        first              <- worker.settle(settlement(batch.claim, NonEmptyChunk(batch.messages(0).id -> Verdict.Done)))
        // Still owed the second, so the key is nobody else's yet.
        blocked            <- other.claim(Demand(queue, 1.second, 1))
        second             <- worker.settle(settlement(batch.claim, NonEmptyChunk(batch.messages(1).id -> Verdict.Done)))
        after              <- other.claim(Demand(queue, 1.second, 1))
      yield assertTrue(first, second, blocked.isEmpty, after.isEmpty) // nothing left, so nothing to claim
    },
    test("a nacked message stays in its place, and the key comes back when nothing is owed") {
      // Producer order survives a settle in any order: 1 acked, 2 nacked, 4 acked, 3 nacked leaves 2 and 3
      // where the producer put them.
      for
        (worker, _, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue           = QueueName("mixed")
        key             = MessageKey("k1")
        _              <- ZIO.foreachDiscard(List("1", "2", "3", "4", "5"))(m => worker.enqueue(Submission(queue, message(key, m))))
        held           <- worker.claim(Demand(queue, 2.seconds, 4))
        batch           = held.get
        by              = batch.messages.map(owned => cargo(owned.message) -> owned.id).toMap
        _              <- worker.settle(settlement(batch.claim, NonEmptyChunk(by("1") -> Verdict.Done)))
        _              <- worker.settle(settlement(batch.claim, NonEmptyChunk(by("2") -> Verdict.Failed)))
        _              <- worker.settle(settlement(batch.claim, NonEmptyChunk(by("4") -> Verdict.Done)))
        _              <- worker.settle(settlement(batch.claim, NonEmptyChunk(by("3") -> Verdict.Failed)))
        again          <- worker.claim(Demand(queue, 2.seconds, 5))
      yield assertTrue(
        again.map(body) == Some(Chunk("2", "3", "5")),                       // producer order, whatever order they were settled in
        again.map(_.messages.map(_.attempt).toChunk) == Some(Chunk(2, 2, 1)), // the nacked ones carry their count
      )
    },
    test("a settle naming a message the claim does not own changes nothing") {
      for
        (worker, _, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue           = QueueName("unowned")
        key             = MessageKey("k1")
        _              <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(Submission(queue, message(key, m))))
        held           <- worker.claim(Demand(queue, 2.seconds, 1))
        batch           = held.get
        // "b" is queued but not owned by this claim, and "nowhere" is nobody's.
        applied        <- worker.settle(
                            settlement(
                              batch.claim,
                              NonEmptyChunk(MessageId("b") -> Verdict.Done, MessageId("nowhere") -> Verdict.Done),
                            )
                          )
        // Still owed "a", so the claim is alive and the key is still held.
        _              <- worker.settle(settlement(batch.claim, acks(batch)))
        after          <- worker.claim(Demand(queue, 2.seconds, 2))
      yield assertTrue(applied, after.map(body) == Some(Chunk("b")))
    },
    test("a dequeue's patience covers queueing for a connection, not only the wait for work") {
      // The pool here has one claiming connection, so the second claim queues behind the first. Both must
      // still answer within one patience: a caller cannot tell whether it waited on Redis or on a
      // connection, and `max_wait` is what the service promised it.
      for
        (worker, _, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue           = QueueName("patience")
        patience        = 2.seconds
        demand          = Demand(queue, patience, batch = 1)
        result         <- worker.claim(demand).zipPar(worker.claim(demand)).timed
        (elapsed, both) = result
      yield assertTrue(both._1.isEmpty, both._2.isEmpty, elapsed < patience + 1.second)
    },
    test("the same id enqueued twice for a key is one message") {
      // HSETNX in produce.lua: a producer retrying an at-least-once send must not double the work.
      for
        (worker, _, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue           = QueueName("idempotent")
        key             = MessageKey("k1")
        first          <- worker.enqueue(Submission(queue, message(key, "once")))
        again          <- worker.enqueue(Submission(queue, message(key, "once")))
        held           <- worker.claim(Demand(queue, 2.seconds, 5))
      yield assertTrue(first == 1L, again == 1L, held.map(body) == Some(Chunk("once")))
    },
    test("acknowledging clears the payload as well as the place in line") {
      for
        (worker, _, redis) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue               = QueueName("cleanup")
        key                 = MessageKey("k1")
        _                  <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(Submission(queue, message(key, m))))
        held               <- worker.claim(Demand(queue, 2.seconds, 2))
        _                  <- ZIO.foreachDiscard(held)(batch => worker.settle(settlement(batch.claim, acks(batch))))
        payloads           <- ZIO.attemptBlocking(redis.hlen(Namespace(QueueName("cleanup"), buckets).payloads(MessageKey("k1")))).orDie
        owned              <- ZIO.attemptBlocking(redis.scard(Namespace(QueueName("cleanup"), buckets).owned(MessageKey("k1")))).orDie
        // Idle is the absence of the key from every structure — there is no state entry to check any more.
        claimed            <- ZIO.attemptBlocking(redis.zcard(Namespace(QueueName("cleanup"), buckets).claimed)).orDie
        ready              <- ZIO.attemptBlocking(redis.zcard(Namespace(QueueName("cleanup"), buckets).ready)).orDie
      yield assertTrue(payloads == 0L, owned == 0L, claimed == 0L, ready == 0L)
    },
    test("a claim reclaimed while a nack's backoff is pending queues its key once, not twice") {
      // A partial nack can set a backoff and leave the claim alive. If that claim then lapses, the reclaim
      // and the due-sweep would each push the key onto `ready` — two entries, two claimers, and one of them
      // working for nothing. The fence would stop it corrupting anything; it would still be waste.
      for
        (worker, sweeper, redis) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue                     = QueueName("backoff-reclaim")
        key                       = MessageKey("k1")
        _                        <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(Submission(queue, message(key, m))))
        held                     <- worker.claim(Demand(queue, 2.seconds, 2))
        batch                     = held.get
        // Nack the first with a backoff; the second stays owed, so the claim lives on.
        _                        <- worker.settle(settlement(batch.claim, NonEmptyChunk(batch.messages(0).id -> Verdict.Failed), 1.second))
        // Let the lease lapse and the backoff fall due, then sweep both in one pass.
        _                        <- ZIO.sleep(leaseTtl + 1.second)
        _                        <- sweeper.sweep(queue, 100)
        ready                    <- ZIO.attemptBlocking(redis.zcard(Namespace(queue, buckets).ready)).orDie
      yield assertTrue(ready == 1L)
    },
    test("a heartbeat renews what is held and names what is lost") {
      for
        (worker, sweeper, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue                 = QueueName("beat")
        _                    <- worker.enqueue(Submission(queue, message(MessageKey("k1"), "work")))
        held                 <- worker.claim(Demand(queue, 2.seconds, 1))
        ghost                 = Claim(queue, MessageKey("gone"), Token(7))
        renewed              <- worker.renew(Chunk.fromIterable(held.map(_.claim)) :+ ghost)
        (until, stale)        = renewed
      yield assertTrue(
        stale.map(_.key.toString) == Chunk("gone"), // only the one that was never held
        until.toEpochMilli > 0L,
      )
    },
  ).provideShared(substrate) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.timeout(3.minutes)
