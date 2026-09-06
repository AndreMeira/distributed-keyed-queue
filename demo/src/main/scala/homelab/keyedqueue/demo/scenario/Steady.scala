package homelab.keyedqueue.demo.scenario


import homelab.keyedqueue.demo.{ Scenario, Servers }
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
  override val run: ZIO[Servers & Scope, Throwable, Unit] =
    for
      _ <- ZIO.foreachParDiscard(1 to consumers)(worker => consume(worker).forever).forkScoped
      _ <- produce(0).repeat(Schedule.spaced(pace)).timeout(duration)
    yield ()

  /**
   * One message onto a key chosen at random, so the load spreads without being uniform.
   *
   * @param worker which instance to send through
   * @return noop
   */
  private def produce(worker: Int): ZIO[Servers, Throwable, Unit] =
    for
      client <- ZIO.serviceWith[Servers](_(worker))
      key    <- Random.nextIntBounded(keys).map(index => s"k$index")
      body   <- Random.nextUUID.map(_.toString.take(8))
      _      <- client.enqueue(queue, key, body)
    yield ()

  /**
   * Claim, pretend to work, settle — what a correct consumer does, minus the error handling.
   *
   * @param worker which instance this consumer talks to, for its whole life
   * @return noop
   */
  private def consume(worker: Int): ZIO[Servers, Throwable, Unit] =
    for
      client  <- ZIO.serviceWith[Servers](_(worker))
      claimed <- client.dequeue(queue, 5.seconds)
      _       <- ZIO.foreachDiscard(claimed): work =>
                   ZIO.sleep(20.millis) *> client.settle(work.receipt, work.ids, succeeded = true)
    yield ()
