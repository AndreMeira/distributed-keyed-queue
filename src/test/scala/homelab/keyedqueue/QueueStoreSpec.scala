package homelab.keyedqueue


import homelab.keyedqueue.domain.model.ClaimRef
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis.{ Connection, RedisQueueStore, Scripts }
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
  private val substrate: ZLayer[Any, Any, (QueueStore, QueueStore)] =
    ZLayer.scoped:
      for
        container <- ZIO.acquireRelease(
                       ZIO.attemptBlocking:
                         // Docker Engine 29 rejects the API version docker-java negotiates by default with
                         // an HTTP 400; pinning it is what the toolkit's Testcontainers specs do too.
                         val _ = java.lang.System.setProperty("api.version", "1.40")
                         // GenericContainer is self-referentially generic (SELF extends GenericContainer),
                         // which Scala infers as Nothing — hence the explicit wildcard and no chaining.
                         val started: GenericContainer[?] = GenericContainer("valkey/valkey:8.1-alpine")
                         started.setExposedPorts(java.util.List.of(Integer.valueOf(6379)))
                         started.start()
                         started
                     )(container => ZIO.attemptBlocking(container.stop()).ignore)
        url        = s"redis://${container.getHost}:${container.getMappedPort(6379)}"
        config     = QueueConfig(url, 0, leaseTtl, 1.second, 100, 2, 5.seconds)
        client    <- Connection.client(url)
        first     <- store(client, config, "w1")
        second    <- store(client, config, "sweeper")
      yield (first: QueueStore, second: QueueStore)

  private def store(client: io.lettuce.core.RedisClient, config: QueueConfig, id: String) =
    for
      redis   <- Connection.open(client, config.maxWait)
      scripts <- Scripts.load(redis)
      worker   = RedisQueueStore(redis, scripts, WorkerId(id), config.leaseTtl)
      _       <- worker.renew(Chunk.empty) // register before claiming, as every claimer must
    yield worker

  private def bytes(text: String): Chunk[Byte] = Chunk.fromArray(text.getBytes("UTF-8"))
  private def text(payload: Chunk[Byte]): String = String(payload.toArray, "UTF-8")

  def spec: Spec[TestEnvironment & Scope, Any] = suite("QueueStore over Redis")(
    test("a key's messages are delivered oldest first") {
      for
        (worker, _) <- ZIO.service[(QueueStore, QueueStore)]
        queue        = QueueName("order")
        key          = MessageKey("k1")
        _           <- ZIO.foreachDiscard(List("a", "b", "c"))(m => worker.enqueue(queue, key, bytes(m)))
        seen        <- ZIO.foreach(1 to 3): _ =>
                         for
                           claimed <- worker.claim(queue, 2.seconds)
                           message  = claimed.map(c => text(c.payload)).getOrElse("")
                           _       <- ZIO.foreachDiscard(claimed)(c => worker.settle(c.claim, Verdict.Done, Duration.Zero))
                         yield message
      yield assertTrue(seen == Chunk("a", "b", "c"))
    },
    test("a key being worked is not handed to anybody else, and its next message waits") {
      // The invariant the whole design is built around. While k1 is held, a second claim must find k2 —
      // never k1 again — even though k1 has more messages queued.
      for
        (worker, other) <- ZIO.service[(QueueStore, QueueStore)]
        queue            = QueueName("exclusive")
        _               <- worker.enqueue(queue, MessageKey("k1"), bytes("first"))
        _               <- worker.enqueue(queue, MessageKey("k1"), bytes("second"))
        _               <- worker.enqueue(queue, MessageKey("k2"), bytes("other"))
        held            <- worker.claim(queue, 2.seconds)
        next            <- other.claim(queue, 2.seconds)
        third           <- other.claim(queue, 500.millis)
      yield assertTrue(
        held.map(c => text(c.payload)).contains("first"),
        next.map(c => text(c.payload)).contains("other"),   // k2, because k1 is held
        third.isEmpty,                                       // and k1's second message is not offered
      )
    },
    test("a failure returns the message, keeps its place, and counts the attempt") {
      for
        (worker, _) <- ZIO.service[(QueueStore, QueueStore)]
        queue        = QueueName("retry")
        key          = MessageKey("k1")
        _           <- worker.enqueue(queue, key, bytes("poison"))
        first       <- worker.claim(queue, 2.seconds)
        _           <- ZIO.foreachDiscard(first)(c => worker.settle(c.claim, Verdict.Failed, Duration.Zero))
        second      <- worker.claim(queue, 2.seconds)
      yield assertTrue(
        first.map(_.attempt).contains(1),
        second.map(c => text(c.payload)).contains("poison"), // the same message, not the next one
        second.map(_.attempt).contains(2),                   // and the delivery count shows it
      )
    },
    test("a settle after the lease lapsed is rejected, and the message is redelivered") {
      // The fencing token, end to end: the holder goes silent, the sweep revokes the claim, and the late
      // settle must not apply — otherwise it would re-queue a key somebody else may already hold.
      for
        (worker, sweeper) <- ZIO.service[(QueueStore, QueueStore)]
        queue              = QueueName("fenced")
        _                 <- worker.enqueue(queue, MessageKey("k1"), bytes("work"))
        held              <- worker.claim(queue, 2.seconds)
        _                 <- ZIO.sleep(leaseTtl + 500.millis)         // stop heartbeating: the lease lapses
        swept             <- sweeper.sweep(queue, 100)
        late              <- ZIO.foreach(held)(c => worker.settle(c.claim, Verdict.Done, Duration.Zero))
        again             <- sweeper.claim(queue, 2.seconds)
      yield assertTrue(
        swept.reclaimed.map(_.toString) == Chunk("k1"),
        late.contains(false),                                 // rejected: the token is stale
        again.map(c => text(c.payload)).contains("work"),     // and the work is back
      )
    },
    test("a heartbeat renews what is held and names what is lost") {
      for
        (worker, sweeper) <- ZIO.service[(QueueStore, QueueStore)]
        queue              = QueueName("beat")
        _                 <- worker.enqueue(queue, MessageKey("k1"), bytes("work"))
        held              <- worker.claim(queue, 2.seconds)
        ghost              = ClaimRef(queue, MessageKey("gone"), Token(7))
        renewed           <- worker.renew(Chunk.fromIterable(held.map(_.claim)) :+ ghost)
        (until, stale)     = renewed
      yield assertTrue(
        stale.map(_.key.toString) == Chunk("gone"),           // only the one that was never held
        until.toEpochMilli > 0L,
      )
    },
  ).provideShared(substrate) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.timeout(3.minutes)
