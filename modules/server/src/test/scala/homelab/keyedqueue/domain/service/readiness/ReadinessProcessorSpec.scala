package homelab.keyedqueue.domain.service.readiness


import homelab.common.error.ApplicationError
import homelab.common.messaging.Consumer
import homelab.keyedqueue.domain.types.{ LockName, QueueName }
import zio.*
import zio.test.*


/**
 * Where each kind of wake ends up.
 *
 * '''The gap case is here because nothing else exercises it.''' No Redis reader emits one — a stream
 * replays from the id it holds — so without this test the branch would first run in whatever transport
 * needs it, which is the wrong place to discover it.
 */
object ReadinessProcessorSpec extends ZIOSpecDefault:

  private val queue = QueueName("orders")
  private val lock  = LockName("route-7")

  /** An input that never delivers: these tests hand wakes to `process` directly. */
  private val silent: Consumer.Batched[ApplicationError.AdapterError, Wake] = new Consumer.Batched[ApplicationError.AdapterError, Wake]:
    override def consume[E2 >: ApplicationError.AdapterError](logic: List[Wake] => IO[E2, Unit]): IO[E2, Unit] = ZIO.never

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ReadinessProcessor")(
    test("a queue wake reaches the queue's readiness, and not the lock's") {
      ZIO.scoped:
        for
          queueReady <- QueueReadiness.make
          lockReady  <- LockReadiness.make
          mailbox    <- lockReady.subscribe(lock)
          _          <- queueReady.awaitReady(queue, 50.millis)(ZIO.none) // spend the seed token
          _          <- ReadinessProcessor(silent, queueReady, lockReady).process(List(Wake.Queue(queue)))
          found      <- queueReady.awaitReady(queue, 1.second)(ZIO.some(1))
          woken      <- mailbox.take.timeout(100.millis)
        yield assertTrue(found.contains(1), woken.isEmpty)
    },
    test("a lock wake reaches the lock's readiness, and not the queue's") {
      ZIO.scoped:
        for
          queueReady <- QueueReadiness.make
          lockReady  <- LockReadiness.make
          mailbox    <- lockReady.subscribe(lock)
          _          <- queueReady.awaitReady(queue, 50.millis)(ZIO.none)
          _          <- ReadinessProcessor(silent, queueReady, lockReady).process(List(Wake.Lock(lock)))
          woken      <- mailbox.take.timeout(1.second)
          found      <- queueReady.awaitReady(queue, 100.millis)(ZIO.some(1))
        yield assertTrue(woken.isDefined, found.isEmpty)
    },
    test("a gap reaches both, for every name each of them knows") {
      ZIO.scoped:
        for
          queueReady <- QueueReadiness.make
          lockReady  <- LockReadiness.make
          mailbox    <- lockReady.subscribe(lock)
          _          <- queueReady.awaitReady(queue, 50.millis)(ZIO.none)
          _          <- ReadinessProcessor(silent, queueReady, lockReady).process(List(Wake.Gap))
          woken      <- mailbox.take.timeout(1.second)
          found      <- queueReady.awaitReady(queue, 1.second)(ZIO.some(1))
        yield assertTrue(woken.isDefined, found.contains(1))
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(1.minute)
