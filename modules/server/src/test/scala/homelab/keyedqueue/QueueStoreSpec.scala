package homelab.keyedqueue


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ Claim, Claimed, Message }
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis.{ Connection, RedisQueueStore, Scripts }
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

  /** A Valkey container for the suite, and two stores over it — a worker's, and a sweeper's. */
  private val substrate: ZLayer[Any, Any, (QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])] =
    ZLayer.scoped:
      for
        container <- ZIO.acquireRelease(
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
        url        = s"redis://${container.getHost}:${container.getMappedPort(6379)}"
        config     = QueueConfig(url, cluster = false, 0, leaseTtl, 1.second, 100, 1, 5.seconds, maxBatchLimit = 32)
        client    <- Connection.client(url)
        first     <- store(client, config, "w1")
        second    <- store(client, config, "sweeper")
        inspect   <- Connection.open(client, config.maxWait) // to assert on what the adapter wrote
      yield (first: QueueStore, second: QueueStore, inspect)

  /** A store with its own connections — one shared, one claiming — as the service builds one. */
  private def store(client: io.lettuce.core.RedisClient, config: QueueConfig, id: String) =
    for
      connection <- Connection.pool(Connection.open(client, _), config.maxWait, config.maxWait + 10.seconds, config.claimers)
      scripts    <- connection.provide(Scripts.make)
      store      <- RedisQueueStore.make(connection, scripts, WorkerId(id), config.leaseTtl)
    yield store

  /** A message whose cargo is `body`: these tests care about order and ownership, not about content. */
  private def message(key: MessageKey, body: String): Message =
    Message(key, messageId = MessageId(body), payloadType = "test.Text", Encoding.Json, None, Chunk.fromArray(body.getBytes("UTF-8")))

  private def cargo(message: Message): String = String(message.payload.toArray, "UTF-8")

  /** What a batch is carrying, as text, in the order it was handed over. */
  private def body(batch: Claimed): Chunk[String] = batch.messages.map(owned => cargo(owned.message))

  /** Acknowledge everything a batch owns. */
  private def acks(batch: Claimed): Chunk[(MessageId, Verdict)] = batch.messages.map(_.id -> Verdict.Done)

  /** Claim exactly one message, for the tests that are not about batching. */
  private def one(store: QueueStore, queue: QueueName): ZIO[Any, QueueError, Option[Claimed]] =
    store.claim(queue, 2.seconds, maxBatch = 1)

  /** Acknowledge a single-message batch and report what it was carrying. */
  private def ack(store: QueueStore, queue: QueueName)(batch: Option[Claimed]): ZIO[Any, QueueError, String] =
    ZIO
      .foreach(batch)(one => store.settle(one.claim, acks(one), Duration.Zero).as(body(one).mkString))
      .map(_.getOrElse(""))

  /** Report a whole batch as failed. */
  private def nack(store: QueueStore)(batch: Claimed): ZIO[Any, QueueError, Boolean] =
    store.settle(batch.claim, batch.messages.map(_.id -> Verdict.Failed), Duration.Zero)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("QueueStore over Redis")(
    test("a key's messages are delivered oldest first") {
      for
        (worker, _, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue           = QueueName("order")
        key             = MessageKey("k1")
        _              <- ZIO.foreachDiscard(List("a", "b", "c"))(m => worker.enqueue(queue, message(key, m)))
        seen           <- ZIO.foreach(1 to 3)(_ => one(worker, queue).flatMap(ack(worker, queue)))
      yield assertTrue(seen == Chunk("a", "b", "c"))
    },
    test("a key being worked is not handed to anybody else, and its next message waits") {
      // The invariant the whole design is built around. While k1 is held, a second claim must find k2 —
      // never k1's next message, and never k1 again.
      for
        (worker, other, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue               = QueueName("exclusive")
        _                  <- worker.enqueue(queue, message(MessageKey("k1"), "first"))
        _                  <- worker.enqueue(queue, message(MessageKey("k1"), "second"))
        _                  <- worker.enqueue(queue, message(MessageKey("k2"), "other-key"))
        held               <- worker.claim(queue, 2.seconds, maxBatch = 1)
        while_held         <- other.claim(queue, 2.seconds, maxBatch = 1)
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
        _              <- worker.enqueue(queue, message(key, "poison"))
        _              <- worker.enqueue(queue, message(key, "after"))
        first          <- worker.claim(queue, 2.seconds, maxBatch = 1)
        _              <- ZIO.foreachDiscard(first)(nack(worker))
        second         <- worker.claim(queue, 2.seconds, maxBatch = 1)
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
        _                    <- worker.enqueue(queue, message(key, "work"))
        held                 <- worker.claim(queue, 2.seconds, maxBatch = 1)
        _                    <- ZIO.sleep(leaseTtl + 500.millis)
        _                    <- sweeper.sweep(queue, 100)
        late                 <- ZIO.foreach(held)(batch => worker.settle(batch.claim, acks(batch), Duration.Zero))
        again                <- worker.claim(queue, 2.seconds, maxBatch = 1)
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
        _              <- ZIO.foreachDiscard(List("a", "b", "c", "d"))(m => worker.enqueue(queue, message(key, m)))
        held           <- worker.claim(queue, 2.seconds, maxBatch = 3)
      yield assertTrue(
        held.map(body) == Some(Chunk("a", "b", "c")),
        held.map(_.backlogDepth) == Some(1), // d, still queued
        held.map(_.messages.map(_.attempt)) == Some(Chunk(1, 1, 1)),
      )
    },
    test("a batch larger than the key holds returns what there is") {
      for
        (worker, _, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue           = QueueName("batch-short")
        key             = MessageKey("k1")
        _              <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(queue, message(key, m)))
        held           <- worker.claim(queue, 2.seconds, maxBatch = 10)
      yield assertTrue(held.map(body) == Some(Chunk("a", "b")), held.map(_.backlogDepth) == Some(0))
    },
    test("a claim settled piece by piece keeps its key until nothing is owed") {
      for
        (worker, other, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue               = QueueName("partial")
        key                 = MessageKey("k1")
        _                  <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(queue, message(key, m)))
        held               <- worker.claim(queue, 2.seconds, maxBatch = 2)
        batch               = held.get
        first              <- worker.settle(batch.claim, Chunk(batch.messages(0).id -> Verdict.Done), Duration.Zero)
        // Still owed the second, so the key is nobody else's yet.
        blocked            <- other.claim(queue, 1.second, maxBatch = 1)
        second             <- worker.settle(batch.claim, Chunk(batch.messages(1).id -> Verdict.Done), Duration.Zero)
        after              <- other.claim(queue, 1.second, maxBatch = 1)
      yield assertTrue(first, second, blocked.isEmpty, after.isEmpty) // nothing left, so nothing to claim
    },
    test("a nacked message stays in its place, and the key comes back when nothing is owed") {
      // Producer order survives a settle in any order: 1 acked, 2 nacked, 4 acked, 3 nacked leaves 2 and 3
      // where the producer put them.
      for
        (worker, _, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue           = QueueName("mixed")
        key             = MessageKey("k1")
        _              <- ZIO.foreachDiscard(List("1", "2", "3", "4", "5"))(m => worker.enqueue(queue, message(key, m)))
        held           <- worker.claim(queue, 2.seconds, maxBatch = 4)
        batch           = held.get
        by              = batch.messages.map(owned => cargo(owned.message) -> owned.id).toMap
        _              <- worker.settle(batch.claim, Chunk(by("1") -> Verdict.Done), Duration.Zero)
        _              <- worker.settle(batch.claim, Chunk(by("2") -> Verdict.Failed), Duration.Zero)
        _              <- worker.settle(batch.claim, Chunk(by("4") -> Verdict.Done), Duration.Zero)
        _              <- worker.settle(batch.claim, Chunk(by("3") -> Verdict.Failed), Duration.Zero)
        again          <- worker.claim(queue, 2.seconds, maxBatch = 5)
      yield assertTrue(
        again.map(body) == Some(Chunk("2", "3", "5")),               // producer order, whatever order they were settled in
        again.map(_.messages.map(_.attempt)) == Some(Chunk(2, 2, 1)), // the nacked ones carry their count
      )
    },
    test("a settle naming a message the claim does not own changes nothing") {
      for
        (worker, _, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue           = QueueName("unowned")
        key             = MessageKey("k1")
        _              <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(queue, message(key, m)))
        held           <- worker.claim(queue, 2.seconds, maxBatch = 1)
        batch           = held.get
        // "b" is queued but not owned by this claim, and "nowhere" is nobody's.
        applied        <- worker.settle(
                            batch.claim,
                            Chunk(MessageId("b") -> Verdict.Done, MessageId("nowhere") -> Verdict.Done),
                            Duration.Zero,
                          )
        // Still owed "a", so the claim is alive and the key is still held.
        _              <- worker.settle(batch.claim, acks(batch), Duration.Zero)
        after          <- worker.claim(queue, 2.seconds, maxBatch = 2)
      yield assertTrue(applied, after.map(body) == Some(Chunk("b")))
    },
    test("the same id enqueued twice for a key is one message") {
      // HSETNX in produce.lua: a producer retrying an at-least-once send must not double the work.
      for
        (worker, _, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue           = QueueName("idempotent")
        key             = MessageKey("k1")
        first          <- worker.enqueue(queue, message(key, "once"))
        again          <- worker.enqueue(queue, message(key, "once"))
        held           <- worker.claim(queue, 2.seconds, maxBatch = 5)
      yield assertTrue(first == 1L, again == 1L, held.map(body) == Some(Chunk("once")))
    },
    test("acknowledging clears the payload as well as the place in line") {
      for
        (worker, _, redis) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue               = QueueName("cleanup")
        key                 = MessageKey("k1")
        _                  <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(queue, message(key, m)))
        held               <- worker.claim(queue, 2.seconds, maxBatch = 2)
        _                  <- ZIO.foreachDiscard(held)(batch => worker.settle(batch.claim, acks(batch), Duration.Zero))
        payloads           <- ZIO.attemptBlocking(redis.hlen("{q:cleanup}:payloads:k1")).orDie
        owned              <- ZIO.attemptBlocking(redis.scard("{q:cleanup}:owned:k1")).orDie
        state              <- ZIO.attemptBlocking(redis.hget("{q:cleanup}:state", "k1")).orDie
        ready              <- ZIO.attemptBlocking(redis.llen("{q:cleanup}:ready")).orDie
      yield assertTrue(payloads == 0L, owned == 0L, state == null, ready == 0L)
    },
    test("a claim reclaimed while a nack's backoff is pending queues its key once, not twice") {
      // A partial nack can set a backoff and leave the claim alive. If that claim then lapses, the reclaim
      // and the due-sweep would each push the key onto `ready` — two entries, two claimers, and one of them
      // working for nothing. The fence would stop it corrupting anything; it would still be waste.
      for
        (worker, sweeper, redis) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue                     = QueueName("backoff-reclaim")
        key                       = MessageKey("k1")
        _                        <- ZIO.foreachDiscard(List("a", "b"))(m => worker.enqueue(queue, message(key, m)))
        held                     <- worker.claim(queue, 2.seconds, maxBatch = 2)
        batch                     = held.get
        // Nack the first with a backoff; the second stays owed, so the claim lives on.
        _                        <- worker.settle(batch.claim, Chunk(batch.messages(0).id -> Verdict.Failed), 1.second)
        // Let the lease lapse and the backoff fall due, then sweep both in one pass.
        _                        <- ZIO.sleep(leaseTtl + 1.second)
        _                        <- sweeper.sweep(queue, 100)
        ready                    <- ZIO.attemptBlocking(redis.llen("{q:backoff-reclaim}:ready")).orDie
      yield assertTrue(ready == 1L)
    },
    test("a claimer registers in the queue it claims from, not somewhere else") {
      // White-box on purpose, because the failure is invisible from outside: a claimer registered under the
      // wrong namespace still claims happily, and only the recovery path is broken — its in-transition keys
      // would be unrecoverable, and nothing would say so until a worker died at exactly the wrong moment.
      for
        (worker, _, redis) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue               = QueueName("registered")
        _                  <- worker.enqueue(queue, message(MessageKey("k1"), "x"))
        claimed            <- worker.claim(queue, 2.seconds, maxBatch = 1)
        here               <- ZIO.attemptBlocking(redis.zcard("{q:registered}:workers")).orDie
        nowhere            <- ZIO.attemptBlocking(redis.zcard("{q:}:workers")).orDie
      yield assertTrue(
        claimed.isDefined,
        here == 1L,   // the claimer announced itself where the sweep of this queue will look
        nowhere == 0L, // and not in the namespace of a queue that does not exist
      )
    },
    test("a heartbeat renews what is held and names what is lost") {
      for
        (worker, sweeper, _) <- ZIO.service[(QueueStore, QueueStore, RedisClusterCommands[String, Array[Byte]])]
        queue                 = QueueName("beat")
        _                    <- worker.enqueue(queue, message(MessageKey("k1"), "work"))
        held                 <- worker.claim(queue, 2.seconds, maxBatch = 1)
        ghost                 = Claim(queue, MessageKey("gone"), Token(7))
        renewed              <- worker.renew(Chunk.fromIterable(held.map(_.claim)) :+ ghost)
        (until, stale)        = renewed
      yield assertTrue(
        stale.map(_.key.toString) == Chunk("gone"), // only the one that was never held
        until.toEpochMilli > 0L,
      )
    },
  ).provideShared(substrate) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.timeout(3.minutes)
