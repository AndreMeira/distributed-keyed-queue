package homelab.keyedqueue.demo.scenario


import homelab.keyedqueue.demo.{ Scenario, Servers }
import zio.*


/**
 * Consumers waiting on an empty queue, woken one message at a time.
 *
 * The scenario that makes `observability.md`'s warning concrete: most dequeues here wait out their whole
 * patience and a few return at once, so the latency histogram is two populations rather than one
 * distribution. A panel that reads a p99 near the patience as a regression is misreading exactly this.
 *
 * It is also the only way to watch the wake path do its job — the gap between the enqueue and the delivery
 * is the whole of what the signal and the wake stream exist to shorten.
 */
object Idle extends Scenario:

  private val queue     = "demo-idle"
  private val consumers = 8
  private val messages  = 10
  private val patience  = 20.seconds
  private val gap       = 5.seconds

  override val name: String = "idle"

  override val lookAt: String =
    s"""the dequeue latency panel, and a trace per message.
       |    · $consumers consumers park on an empty queue; one message arrives every ${gap.toSeconds}s
       |    · latency splits in two: the woken consumer returns in milliseconds, the other ${consumers - 1}
       |      wait out ${patience.toSeconds}s and return nothing. Both are correct
       |    · in Jaeger, one trace per message: QueueService.dequeue over RedisQueueStore.attempt""".stripMargin

  /**
   * Park every consumer on an empty queue, then wake them one message at a time.
   *
   * @return noop once the last message has been sent; fails when a call to the service does
   */
  override val run: ZIO[Servers & Scope, Throwable, Unit] =
    for
      _ <- ZIO.foreachParDiscard(1 to consumers)(worker => consume(worker).forever).forkScoped
      _ <- ZIO.sleep(2.seconds) // let them all park before anything arrives
      _ <- ZIO.foreachDiscard(1 to messages): index =>
             // Sent through a different instance each time, so most messages have to cross from the one
             // that took the enqueue to the one a consumer is parked on — which is the wake path's job.
             produce(index, s"k$index") *> ZIO.sleep(gap)
    yield ()

  /**
   * One message for a key nobody has used, so each wakes exactly one waiting consumer.
   *
   * @param worker which instance to send through
   * @param key the key to send under
   * @return noop
   */
  private def produce(worker: Int, key: String): ZIO[Servers, Throwable, Unit] =
    ZIO.serviceWithZIO[Servers](_(worker).enqueue(queue, key, "wake").unit)

  /**
   * Wait the full patience, settle whatever arrives.
   *
   * @param worker which instance this consumer parks on, for its whole life
   * @return noop
   */
  private def consume(worker: Int): ZIO[Servers, Throwable, Unit] =
    for
      client  <- ZIO.serviceWith[Servers](_(worker))
      claimed <- client.dequeue(queue, patience)
      _       <- ZIO.foreachDiscard(claimed): work =>
                   client.settle(work.receipt, work.ids, succeeded = true)
    yield ()
