package homelab.keyedqueue.e2e


import homelab.keyedqueue.v1.{ Applied, MessageOutcome, Outcome }
import zio.*
import zio.test.*


/**
 * dkq as it is deployed: two instances, one Valkey, clients over the wire.
 *
 * '''What makes this different from `GrpcSpec`.''' That suite runs the service in the test's own JVM against
 * a container, and proves the API behaves. This one proves the things that only exist in a *deployment*:
 * that two instances are one queue rather than two, that a client parked on one is woken by a client talking
 * to the other, and that killing an instance loses no work. None of those can fail in a single-process test,
 * which is exactly why they need this one.
 *
 * Every assertion here is made through the generated client, over TCP, against a service in a container —
 * there is no seam anywhere in this file where a test could be passing for a reason the deployment does not
 * share.
 *
 * Run it with `sbt e2e`, which builds the image first. Set `DKQ_E2E_ENDPOINTS=host:port,host:port` to run
 * the same suite against a deployment that already exists, and `DKQ_E2E_KEEP=1` to leave the stack up
 * afterwards for a post-mortem.
 */
object EndToEndSpec extends ZIOSpecDefault:

  /** The instances' lease, from `docker-compose.e2e.yml`; the waits below are all reasoned from it. */
  private val lease = 5.seconds

  def spec: Spec[TestEnvironment & Scope, Any] = suite("dkq, deployed")(
    test("a message enqueued on one instance is dequeued from the other") {
      // The cheapest possible proof that this is one queue and not two.
      for
        dkq      <- ZIO.service[Deployment]
        _        <- dkq.a.enqueue(dkq.queue("roundtrip"), "k1", "hello")
        delivery <- dkq.b.dequeue(dkq.queue("roundtrip"), 5.seconds)
        applied  <- dkq.b.settle(delivery)
        drained  <- dkq.a.dequeue(dkq.queue("roundtrip"), 1.second)
      yield assertTrue(
        Instance.claimed(delivery).flatMap(_.message).map(_.payload.toStringUtf8).contains("hello"),
        delivery.head.map(_.attempt).contains(1),
        applied == Applied.APPLIED_OK,
        drained.head.isEmpty, // and settling on b really removed it, as seen from a
      )
    },
    test("a dequeue parked on one instance is woken by an enqueue on the other") {
      // The roadmap's own exit criterion for phase 1. The elapsed time is the assertion that matters: it
      // proves the parked call was *woken*, rather than having polled or waited out its patience.
      for
        dkq                 <- ZIO.service[Deployment]
        parked              <- dkq.a.dequeue(dkq.queue("wake"), 20.seconds).timed.fork
        _                   <- ZIO.sleep(2.seconds) // long enough that the dequeue is certainly parked
        _                   <- dkq.b.enqueue(dkq.queue("wake"), "k1", "wake up")
        (elapsed, delivery) <- parked.join
        _                   <- dkq.a.settle(delivery)
      yield assertTrue(
        Instance.claimed(delivery).flatMap(_.message).map(_.payload.toStringUtf8).contains("wake up"),
        elapsed >= 1900.millis, // it really blocked, rather than finding the message already there
        elapsed <= 10.seconds,  // and it was woken, rather than waiting out its 20
      )
    },
    test("a key's messages keep their order, across instances and consumers") {
      val bodies = Chunk.fromIterable(0 until 24).map(_.toString)
      for
        dkq     <- ZIO.service[Deployment]
        _       <- ZIO.foreachDiscard(bodies.zipWithIndex)((body, index) => dkq(index).enqueue(dkq.queue("order"), "k1", body))
        seen    <- Ref.make(Chunk.empty[Consumer.Handled])
        _       <- ZIO.foreachParDiscard(0 until 4)(index => Consumer.drain(dkq(index), dkq.queue("order"), 5.seconds, Duration.Zero, seen))
        handled <- seen.get
      yield assertTrue(
        handled.map(_.body) == bodies,             // FIFO, though four consumers on two instances competed
        handled.map(_.instance).distinct.size == 2, // and both instances really served some of it
      )
    },
    test("one key is never worked by two consumers at once") {
      // The invariant the whole design exists for. Each consumer holds its message for 300ms, so if the
      // queue ever handed one key to two consumers, their windows would overlap by far more than any
      // measurement error — see Consumer for why these windows cannot produce a false positive.
      for
        dkq     <- ZIO.service[Deployment]
        _       <- ZIO.foreachDiscard(0 until 12)(index => dkq(index).enqueue(dkq.queue("exclusive"), "k1", index.toString))
        seen    <- Ref.make(Chunk.empty[Consumer.Handled])
        _       <- ZIO.foreachParDiscard(0 until 4)(index => Consumer.drain(dkq(index), dkq.queue("exclusive"), 5.seconds, 300.millis, seen))
        handled <- seen.get
        windows  = handled.map(one => (one.claimedAt.toEpochMilli, one.releasedAt.toEpochMilli)).sortBy(_._1)
      yield assertTrue(
        handled.size == 12,
        // Chunk#zipWith rather than zip: ZIO's Zippable would flatten the pairs into one 4-tuple.
        windows.zipWith(windows.drop(1))((earlier, later) => earlier._2 <= later._1).forall(identity),
      )
    },
    test("an instance killed mid-handler loses nothing: the claim lapses and the work comes back") {
      // A pod dies holding a claim, which no graceful shutdown can help with — hence SIGKILL. The lease is
      // the only thing standing between that and work disappearing, and the sweep on the *surviving*
      // instance is what notices. The enqueue goes through a so that a's watchdog is already watching this
      // queue when b dies.
      for
        dkq     <- ZIO.service[Deployment]
        _       <- dkq.a.enqueue(dkq.queue("death"), "k1", "survive me")
        held    <- dkq.b.dequeue(dkq.queue("death"), 5.seconds)
        _       <- Compose.kill(dkq.b.name) // no settle, no goodbye: the holder simply vanishes
        again   <- dkq.a.dequeue(dkq.queue("death"), 25.seconds)
        applied <- dkq.a.settle(again)
      yield assertTrue(
        held.head.map(_.attempt).contains(1),
        Instance.claimed(again).flatMap(_.message).map(_.payload.toStringUtf8).contains("survive me"),
        again.head.map(_.attempt).contains(2), // counted, which is what a poison-message policy will read
        applied == Applied.APPLIED_OK,
      )
    } @@ TestAspect.after(Compose.revive("dkq-b").orDie) @@ TestAspect.ifEnvNotSet("DKQ_E2E_ENDPOINTS"),
    test("a heartbeat keeps a claim past its lease") {
      // A handler that runs several times the lease. Only the beats keep the claim alive; the assertion is
      // that the settle at the end is still accepted.
      for
        dkq     <- ZIO.service[Deployment]
        _       <- dkq.a.enqueue(dkq.queue("beats"), "k1", "slow work")
        held    <- dkq.a.dequeue(dkq.queue("beats"), 5.seconds)
        receipt  = held.receipt
        beating <- dkq.a.heartbeat(Seq(receipt)).repeat(Schedule.spaced(lease.dividedBy(3))).fork
        _       <- ZIO.sleep(lease.multipliedBy(3))
        _       <- beating.interrupt
        applied <- dkq.a.settle(held)
      yield assertTrue(applied == Applied.APPLIED_OK)
    },
    test("a claim held in silence is reclaimed, and its settle refused") {
      // The other half: the same handler without beats. Its work is not just lost — the settle it eventually
      // sends must be *refused*, or the message would be completed twice by two different consumers.
      for
        dkq     <- ZIO.service[Deployment]
        _       <- dkq.a.enqueue(dkq.queue("silence"), "k1", "abandoned")
        held    <- dkq.a.dequeue(dkq.queue("silence"), 5.seconds)
        _       <- ZIO.sleep(lease + 5.seconds) // the lease, plus room for the sweep to notice
        applied <- dkq.a.settle(held)
        again   <- dkq.b.dequeue(dkq.queue("silence"), 10.seconds)
        _       <- dkq.b.settle(again)
      yield assertTrue(
        applied == Applied.APPLIED_STALE, // the fence moved on: this outcome is discarded, not applied
        again.head.map(_.attempt).contains(2), // and the message is somebody else's now
      )
    },
    test("a batch claimed on one instance is settled message by message, and its key waits for the last") {
      // What batching is for: the consumer sees several of a key's messages at once, decides which are
      // superseded, and says so one at a time. The key is nobody else's until nothing is owed — which is
      // what keeps per-key exclusivity while more than one message is out.
      for
        dkq     <- ZIO.service[Deployment]
        queue    = dkq.queue("batch")
        _       <- ZIO.foreachDiscard(1 to 3)(n => dkq(0).enqueue(queue, "k1", s"v$n"))
        held    <- dkq(0).dequeue(queue, 2.seconds, maxBatch = 3)
        ids      = Instance.claimed(held).map(_.messageId)
        bodies   = Instance.claimed(held).map(one => one.message.map(_.payload.toStringUtf8))
        // v1 and v2 are superseded by v3; acknowledged without being worked.
        _       <- dkq(0).settleEach(held.receipt, ids.take(2).map(MessageOutcome(_, Outcome.OUTCOME_DONE)))
        // Still owed v3, so the other instance cannot have the key.
        blocked <- dkq(1).dequeue(queue, 1.second)
        _       <- dkq(0).settleEach(held.receipt, ids.drop(2).map(MessageOutcome(_, Outcome.OUTCOME_DONE)))
        drained <- dkq(1).dequeue(queue, 1.second)
      yield assertTrue(
        bodies == Seq(Some("v1"), Some("v2"), Some("v3")), // producer order, across the deployment
        held.backlogDepth == 0,
        blocked.head.isEmpty, // the claim was still alive on the other instance
        drained.head.isEmpty, // and by then the key was empty
      )
    },
    test("under load nothing is lost, nothing is delivered twice, and every key keeps its order") {
      // A saturation check, not a benchmark: eight keys worked by eight consumers across both instances,
      // asserting correctness under concurrent pressure. The throughput it prints is information — there is
      // no rate control here, and no percentiles. Use ghz against the same stack if you want numbers.
      val keys     = Chunk.fromIterable(0 until 8).map(index => s"k$index")
      val perKey   = 15
      val expected = keys.size * perKey
      for
        dkq     <- ZIO.service[Deployment]
        started <- Clock.instant
        _       <- ZIO.foreachParDiscard(keys.zipWithIndex): (key, offset) =>
                     // Sequential within a key — that is what establishes the order being asserted — and
                     // spread across instances, so no key is enqueued through one instance alone.
                     ZIO.foreachDiscard(0 until perKey)(index => dkq(index + offset).enqueue(dkq.queue("load"), key, index.toString))
        seen    <- Ref.make(Chunk.empty[Consumer.Handled])
        _       <- ZIO.foreachParDiscard(0 until 8)(index => Consumer.drain(dkq(index), dkq.queue("load"), 5.seconds, Duration.Zero, seen))
        handled <- seen.get
        // Measured to the last message *handled*, not to when the consumers gave up: every consumer ends by
        // waiting out its patience on an empty queue, and including that would report the patience rather
        // than the throughput.
        finished = handled.map(_.releasedAt.toEpochMilli).maxOption.getOrElse(started.toEpochMilli)
        elapsed  = math.max(finished - started.toEpochMilli, 1)
        rate     = expected * 1000.0 / elapsed
        _       <- Console.printLine(f"load: $expected messages through 2 instances in ${elapsed}ms ($rate%.0f msg/s)")
      yield assertTrue(
        handled.size == expected,
        // No duplicates. At-least-once delivery makes a duplicate legal in principle, but nothing here
        // fails or stalls, so one would mean a lease lapsed under load — worth investigating, not tolerating.
        handled.map(one => (one.key, one.body)).distinct.size == expected,
        keys.forall(key => handled.filter(_.key == key).map(_.body) == Chunk.fromIterable(0 until perKey).map(_.toString)),
      )
    },
  ).provideShared(Deployment.layer)
    @@ TestAspect.withLiveClock // every wait here is a real one; a virtual clock would prove nothing
    @@ TestAspect.sequential // the tests share a deployment, and one of them kills half of it
    @@ TestAspect.timeout(10.minutes)
