package homelab.keyedqueue.e2e


import zio.*
import zio.test.*


/**
 * The saturation check, re-run at several shapes, to say whether a change to the claim path made the stack
 * faster or slower.
 *
 * '''An indicator, not a benchmark.''' There is no rate control, no percentiles, no warm JIT to speak of,
 * and the numbers move with whatever else the laptop is doing. What it is good for is comparing two
 * implementations on one machine, in one sitting — which is exactly the question a change to how a claim is
 * taken raises. For numbers worth quoting, point `ghz` at the stack.
 *
 * '''Off by default''', because it costs minutes and the e2e suite is a gate. Run it deliberately:
 * {{{
 *   DKQ_THROUGHPUT=1 sbt "e2e/testOnly *ThroughputSpec"
 * }}}
 */
object ThroughputSpec extends ZIOSpecDefault:

  /** keys × consumers, chosen to show which of the two binds — see docs/research/throughput-first-numbers.md. */
  private val shapes = Chunk((8, 8), (16, 16), (64, 8), (64, 16), (64, 32))

  /** Messages per key. Enough that startup is not what is being measured. */
  private val perKey = 15

  /** Runs per shape; the median is reported, because the slowest run is usually something else on the box. */
  private val rounds = 3

  def spec: Spec[TestEnvironment & Scope, Any] = suite("indicators")(
    test("how long the first message waits on a queue the instance has only just started watching") {
      // The listener's read names the streams it was issued with, so a queue watched while a read is in
      // flight is not heard until that read returns. This measures what that costs, against a control
      // where the queue has been watched long enough to be in the current read.
      val rounds  = 8
      val waiters = 4

      def probe(tag: String, label: String, grace: Duration) =
        ZIO
          .foreach(0 until rounds): round =>
            val queue = s"cold-$tag-$round" // a queue no instance has ever watched
            for
              seen     <- Ref.make(Chunk.empty[Consumer.Handled])
              dkq      <- ZIO.service[Deployment]
              draining <- ZIO
                            .foreachParDiscard(0 until waiters): index =>
                              Consumer.drain(dkq(index), queue, 3.seconds, Duration.Zero, seen)
                            .fork
              _        <- ZIO.sleep(grace) // parked; the listener may or may not have picked the queue up
              now      <- Clock.instant
              _        <- dkq(0).enqueue(queue, "k", now.toEpochMilli.toString)
              _        <- draining.join
              handled  <- seen.get
            yield handled.map(one => one.claimedAt.toEpochMilli - one.body.toLong).minOption.getOrElse(-1L)
          .map(timings => summary(label, Chunk.fromIterable(timings).sorted))

      for
        cold <- probe("a", "cold queue, message sent 50ms after parking", 50.millis)
        warm <- probe("b", "warm queue, message sent 1s after parking ", 1.second)
        _    <- Console.printLine(cold)
        _    <- Console.printLine(warm)
      yield assertTrue(true)
    },
    test("a sweep over key and consumer counts") {
      for
        dkq     <- ZIO.service[Deployment]
        _       <- Console.printLine("warming up…")
        _       <- ZIO.foreachDiscard(shapes)((keys, consumers) => once(dkq, "warmup", keys, consumers))
        results <- ZIO.foreach(shapes): (keys, consumers) =>
                     ZIO
                       .foreach(1 to rounds)(round => once(dkq, s"sweep-$keys-$consumers-$round", keys, consumers))
                       .map(rates => (keys, consumers, median(rates)))
        _       <- report(results)
      yield assertTrue(results.forall((_, _, rate) => rate > 0.0))
    },
    test("how long a message waits when every consumer is already idle") {
      // The other half of the picture, and the half the sweep cannot see. There, consumers claim work that
      // is already queued and the doorbell is barely used; here nothing is queued, every consumer is parked
      // on a promise, and what is being measured is how long an enqueue takes to reach one of them.
      //
      // The shape is also the one the old design could not serve: 64 consumers across two instances is 32
      // parked per instance, where `DKQ_CLAIMERS` allowed 16 — the rest would have queued for a connection
      // rather than waiting on the queue.
      val consumers = 64
      val messages  = 20
      val gap       = 200.millis
      val patience  = 15.seconds
      for
        dkq       <- ZIO.service[Deployment]
        queue      = dkq.queue("idle")
        seen      <- Ref.make(Chunk.empty[Consumer.Handled])
        // Parked first, and given a moment to actually be waiting, so every message meets an idle consumer
        // rather than one that has not arrived yet.
        draining  <- ZIO.foreachParDiscard(0 until consumers)(index => Consumer.drain(dkq(index), queue, patience, Duration.Zero, seen)).fork
        _         <- ZIO.sleep(1.second)
        // One at a time, each carrying the moment it was sent: the body is the measurement.
        sends     <- Ref.make(Chunk.empty[Long])
        _         <- ZIO.foreachDiscard(0 until messages): index =>
                       Clock.instant.flatMap: now =>
                         dkq(index)
                           .enqueue(queue, s"k$index", now.toEpochMilli.toString)
                           .timed
                           // The enqueue round trip is inside every latency below, so it is measured too:
                           // what is left after subtracting it is what the doorbell and the claim cost.
                           .flatMap((took, _) => sends.update(_ :+ took.toMillis))
                       *> ZIO.sleep(gap)
        _         <- draining.join
        handled   <- seen.get
        latencies  = handled.map(one => one.claimedAt.toEpochMilli - one.body.toLong).sorted
        enqueues  <- sends.get.map(_.sorted)
        _         <- Console.printLine(summary("wake latency", latencies))
        // Printed beside it because it is *inside* it: what is left after subtracting this is what the
        // doorbell and the claim actually cost.
        _         <- Console.printLine(summary("enqueue round trip", enqueues))
      yield assertTrue(
        handled.size == messages,                                  // a lost wake would show up as a lost message
        handled.map(_.body).distinct.size == messages,             // and a double wake as a duplicate
        latencies.lift(latencies.size * 95 / 100).exists(_ < 2000), // p95 well inside the doorbell's block
      )
    }
  ).provideSomeShared[Scope](Deployment.layer)
    @@ TestAspect.withLiveClock
    @@ TestAspect.sequential
    @@ TestAspect.timeout(20.minutes)
    @@ TestAspect.ifEnvSet("DKQ_THROUGHPUT")

  /**
   * One shape, once: fill a fresh queue, drain it with `consumers` consumers, and report the rate.
   *
   * The queue name carries the round so each run starts empty — a shape that inherited another's leftovers
   * would measure the wrong thing.
   *
   * @param dkq the deployment to drive
   * @param name what to call this run's queue
   * @param keys how many keys to spread the messages over
   * @param consumers how many consumers to drain with, round-robin across both instances
   * @return messages per second, measured to the last message handled
   */
  private def once(dkq: Deployment, name: String, keys: Int, consumers: Int): Task[Double] =
    val partitions = Chunk.fromIterable(0 until keys).map(index => s"k$index")
    val expected   = keys * perKey
    for
      queue   <- ZIO.succeed(dkq.queue(name))
      started <- Clock.instant
      _       <- ZIO.foreachParDiscard(partitions.zipWithIndex): (key, offset) =>
                   ZIO.foreachDiscard(0 until perKey)(index => dkq(index + offset).enqueue(queue, key, index.toString))
      seen    <- Ref.make(Chunk.empty[Consumer.Handled])
      _       <- ZIO.foreachParDiscard(0 until consumers)(index => Consumer.drain(dkq(index), queue, 3.seconds, Duration.Zero, seen))
      handled <- seen.get
      // To the last message handled, not to when the consumers gave up: each ends by waiting out its
      // patience on an empty queue, and counting that would report the patience instead of the rate.
      finished = handled.map(_.releasedAt.toEpochMilli).maxOption.getOrElse(started.toEpochMilli)
      elapsed  = math.max(finished - started.toEpochMilli, 1)
    yield expected * 1000.0 / elapsed

  /**
   * The middle run, so one slow round does not become the number.
   *
   * @param rates what each round measured
   * @return the median
   */
  private def median(rates: Seq[Double]): Double = rates.sorted.apply(rates.size / 2)

  /**
   * Print a set of timings in the terms that matter here: the middle, the tail, the worst.
   *
   * @param label what was measured
   * @param timings the measurements in millis, sorted
   * @return the line to print
   */
  private def summary(label: String, timings: Chunk[Long]): String =
    if timings.isEmpty then s"$label: nothing measured"
    else
      val at = (percent: Int) => timings(math.min(timings.size - 1, timings.size * percent / 100))
      f"$label over ${timings.size} messages: median ${at(50)}ms, p95 ${at(95)}ms, max ${timings.last}ms"

  /**
   * Print the sweep as a table, so two runs can be diffed by eye.
   *
   * @param results one row per shape
   * @return noop
   */
  private def report(results: Seq[(Int, Int, Double)]): UIO[Unit] =
    val header = f"${"keys"}%6s ${"consumers"}%10s ${"msg/s"}%10s"
    val rows   = results.map((keys, consumers, rate) => f"$keys%6d $consumers%10d $rate%10.0f")
    Console.printLine((header +: rows).mkString("\n")).orDie
