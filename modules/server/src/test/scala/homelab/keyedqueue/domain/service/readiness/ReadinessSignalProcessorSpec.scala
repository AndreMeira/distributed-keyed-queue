package homelab.keyedqueue.domain.service.readiness


import homelab.common.error.ApplicationError
import homelab.common.messaging.Consumer
import homelab.keyedqueue.domain.types.{ LockName, QueueName }
import zio.*
import zio.test.*


/**
 * Where each kind of wake ends up.
 *
 * The gap case is here because nothing else exercises it: no Redis reader emits one — a stream
 * replays from the id it holds — so without this test the branch would first run in whatever transport
 * needs it, which is the wrong place to discover it.
 */
object ReadinessSignalProcessorSpec extends ZIOSpecDefault:

  private val queue = QueueName("orders")
  private val lock  = LockName("route-7")

  /** An input that never delivers: these tests hand wakes to `process` directly. */
  private val silent: ReadinessSignalConsumer = new ReadinessSignalConsumer:
    override def consume[E2 >: ApplicationError.AdapterError](
      logic: List[ReadinessSignal] => IO[E2, Unit]
    ): IO[E2, Unit] = ZIO.never

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ReadinessSignalProcessor")(
    test("a queue wake reaches the queue's readiness, and not the lock's") {
      ZIO.scoped:
        for
          queueReady <- QueueReadiness.make
          lockReady  <- LockReadiness.make
          signal     <- lockReady.subscribe(lock)
          tokens     <- queueReady.subscribe(queue)
          _          <- tokens.await(50.millis) // spend the seed token
          _          <- ReadinessSignalProcessor(silent, queueReady, lockReady).process(List(ReadinessSignal.Queue(queue)))
          taken      <- tokens.await(1.second)
          woken      <- signal.await.timeout(100.millis)
        yield assertTrue(taken, woken.isEmpty)
    },
    test("a lock wake reaches the lock's readiness, and not the queue's") {
      ZIO.scoped:
        for
          queueReady <- QueueReadiness.make
          lockReady  <- LockReadiness.make
          signal     <- lockReady.subscribe(lock)
          tokens     <- queueReady.subscribe(queue)
          _          <- tokens.await(50.millis)
          _          <- ReadinessSignalProcessor(silent, queueReady, lockReady).process(List(ReadinessSignal.Lock(lock)))
          woken      <- signal.await.timeout(1.second)
          taken      <- tokens.await(100.millis)
        yield assertTrue(woken.isDefined, !taken)
    },
    test("a gap reaches both, for every name each of them knows") {
      ZIO.scoped:
        for
          queueReady <- QueueReadiness.make
          lockReady  <- LockReadiness.make
          signal     <- lockReady.subscribe(lock)
          tokens     <- queueReady.subscribe(queue)
          _          <- tokens.await(50.millis)
          _          <- ReadinessSignalProcessor(silent, queueReady, lockReady).process(List(ReadinessSignal.Gap))
          woken      <- signal.await.timeout(1.second)
          taken      <- tokens.await(1.second)
        yield assertTrue(woken.isDefined, taken)
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(1.minute)
