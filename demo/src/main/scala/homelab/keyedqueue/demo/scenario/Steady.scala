package homelab.keyedqueue.demo.scenario


import homelab.keyedqueue.demo.{ Client, Scenario }
import zio.*


/**
 * Ordinary traffic: a handful of producers, a handful of consumers, nothing pathological.
 *
 * The baseline every other scenario is read against — what the panels look like when the system is simply
 * working. Run this first, and leave it running while you arrange a dashboard.
 */
object Steady extends Scenario:

  private val queue     = "demo-steady"
  private val keys      = 16
  private val consumers = 4
  private val duration  = 60.seconds
  private val pace      = 50.millis

  override val name: String = "steady"

  override val lookAt: String =
    s"""every panel, filling at a constant rate.
       |    · request rate  — flat, ${1000 / pace.toMillis} enqueues/s split across $keys keys
       |    · latency       — enqueue and settle in single-digit ms
       |    · dequeue       — mostly fast: there is nearly always work, so few calls wait""".stripMargin

  /**
   * Consumers first, then a steady drip of messages for a bounded time.
   *
   * @return noop once the drip has finished; fails when a call to the service does
   */
  override val run: ZIO[Client & Scope, Throwable, Unit] =
    for
      _ <- ZIO.foreachParDiscard(1 to consumers)(_ => consume.forever).forkScoped
      _ <- produce.repeat(Schedule.spaced(pace)).timeout(duration)
    yield ()

  /**
   * One message onto a key chosen at random, so the load spreads without being uniform.
   *
   * @return noop
   */
  private val produce: ZIO[Client, Throwable, Unit] =
    for
      client <- ZIO.service[Client]
      key    <- Random.nextIntBounded(keys).map(index => s"k$index")
      body   <- Random.nextUUID.map(_.toString.take(8))
      _      <- client.enqueue(queue, key, body)
    yield ()

  /**
   * Claim, pretend to work, settle — what a correct consumer does, minus the error handling.
   *
   * @return noop
   */
  private val consume: ZIO[Client, Throwable, Unit] =
    for
      client  <- ZIO.service[Client]
      claimed <- client.dequeue(queue, 5.seconds)
      _       <- ZIO.foreachDiscard(claimed): work =>
                   ZIO.sleep(20.millis) *> client.settle(work.receipt, work.ids, succeeded = true)
    yield ()
