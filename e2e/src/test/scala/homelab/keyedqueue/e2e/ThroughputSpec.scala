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

  def spec: Spec[TestEnvironment & Scope, Any] = suite("throughput")(
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
   * Print the sweep as a table, so two runs can be diffed by eye.
   *
   * @param results one row per shape
   * @return noop
   */
  private def report(results: Seq[(Int, Int, Double)]): UIO[Unit] =
    val header = f"${"keys"}%6s ${"consumers"}%10s ${"msg/s"}%10s"
    val rows   = results.map((keys, consumers, rate) => f"$keys%6d $consumers%10d $rate%10.0f")
    Console.printLine((header +: rows).mkString("\n")).orDie
