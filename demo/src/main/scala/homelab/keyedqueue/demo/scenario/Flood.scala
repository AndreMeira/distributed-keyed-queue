package homelab.keyedqueue.demo.scenario


import homelab.keyedqueue.demo.{ Client, Scenario }
import zio.*


/**
 * As fast as producers can push, against consumers that do no work at all.
 *
 * '''The backlog is the point.''' Producers run flat out on many fibers; consumers claim, settle, and
 * immediately claim again with no handler in between. Whatever queues up is therefore the service's own
 * ceiling rather than a slow handler's — this is the scenario that answers "how fast is dkq itself".
 *
 * `backlogDepth` is what to watch: an enqueue answers with how many messages are queued behind the one it
 * just added, so a rising number means production is outpacing consumption and a flat one means the system
 * is keeping up. It is printed as the run goes, because no panel shows it — the field exists in the
 * response and nowhere else.
 *
 * Keys outnumber consumers deliberately. Per-key exclusivity means one key can only ever be worked by one
 * consumer at a time, so a flood over too few keys would measure that constraint instead of the service.
 */
object Flood extends Scenario:

  private val queue     = "demo-flood"
  private val keys      = 64
  private val producers = 8
  private val consumers = 16
  private val batch     = 8
  private val duration  = 30.seconds

  override val name: String = "flood"

  override val lookAt: String =
    s"""throughput and backlog, with nothing throttling either side.
       |    · request rate  — as high as the box allows; $producers producers, $consumers consumers, $keys keys
       |    · latency       — enqueue and settle climb once the event loop saturates; that IS the ceiling
       |    · dequeue       — fast throughout: there is always work, so nothing waits
       |    · the console   — backlog depth every 5s. Flat means keeping up, rising means it cannot""".stripMargin

  /**
   * Consumers first so nothing accumulates before they start, then producers flat out for a bounded time.
   *
   * @return noop once the flood stops; fails when a call to the service does
   */
  override val run: ZIO[Client & Scope, Throwable, Unit] =
    for
      sent    <- Ref.make(0L)
      deepest <- Ref.make(0L)
      _       <- ZIO.foreachParDiscard(1 to consumers)(_ => consume.forever).forkScoped
      _       <- report(sent, deepest).forkScoped
      _       <- ZIO
                   .foreachParDiscard(1 to producers)(_ => produce(sent, deepest).forever)
                   .timeout(duration)
      total   <- sent.get
      peak    <- deepest.get
      _       <- Console.printLine(
                   s"  sent $total in ${duration.toSeconds}s (${total / duration.toSeconds}/s), deepest backlog seen: $peak"
                 )
    yield ()

  /**
   * One message, as fast as the call returns.
   *
   * The depth the service answers with is recorded rather than discarded: it is the only view of the
   * backlog a consumer of this API has, and the thing this scenario exists to watch.
   *
   * @param sent counts what has gone out
   * @param deepest keeps the high-water mark of the backlog
   * @return noop
   */
  private def produce(sent: Ref[Long], deepest: Ref[Long]): ZIO[Client, Throwable, Unit] =
    for
      client <- ZIO.service[Client]
      key    <- Random.nextIntBounded(keys).map(index => s"k$index")
      body   <- Random.nextLong.map(_.toHexString)
      depth  <- client.enqueue(queue, key, body)
      _      <- sent.update(_ + 1)
      _      <- deepest.update(_ max depth)
    yield ()

  /**
   * Claim a batch and settle it at once — no handler, no sleep.
   *
   * A batch rather than one message at a time, because a consumer that can keep up should be allowed to:
   * one claim covering several of a key's messages is one round trip instead of several, and this scenario
   * is about how much the service can move rather than how carefully.
   *
   * @return noop
   */
  private val consume: ZIO[Client, Throwable, Unit] =
    for
      client  <- ZIO.service[Client]
      claimed <- client.dequeue(queue, 2.seconds, batch)
      _       <- ZIO.foreachDiscard(claimed)(work => client.settle(work.receipt, work.ids, succeeded = true))
    yield ()

  /**
   * Print progress every five seconds, because the interesting number is not on any panel.
   *
   * @param sent what has gone out so far
   * @param deepest the high-water backlog
   * @return never completes; killed with the scope
   */
  private def report(sent: Ref[Long], deepest: Ref[Long]): ZIO[Any, java.io.IOException, Nothing] =
    (for
      before <- sent.get
      _      <- ZIO.sleep(5.seconds)
      after  <- sent.get
      peak   <- deepest.get
      _      <- Console.printLine(f"    ${(after - before) / 5}%5d msg/s   backlog high-water $peak%d")
    yield ()).forever
