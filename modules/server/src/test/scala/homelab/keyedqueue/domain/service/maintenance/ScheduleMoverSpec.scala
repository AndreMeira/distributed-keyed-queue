package homelab.keyedqueue.domain.service.maintenance


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.usecase.queue.InMemoryQueueStore
import zio.*
import zio.test.*


/**
 * The schedule mover over an in-memory store whose answers a test queues up, and a real timer on the
 * `TestClock`: when it moves, and what it asks the timer for afterwards.
 */
object ScheduleMoverSpec extends ZIOSpecDefault:
  import Support.*

  def spec = suite("ScheduleMover")(
    test("it moves once as soon as it starts, on the timer's boot tick") {
      for
        store <- started
        _     <- movedTimes(store, 1)
      yield assertCompletes
    },
    test("it asks the timer for a tick when the store says the next message falls due") {
      for
        store <- InMemoryQueueStore.make
        _     <- store.movesWith(Right(QueueStore.Moved(0, 0, full = false, nextDueIn = Some(10.seconds), failed = 0)))
        _     <- moving(store)
        _     <- movedTimes(store, 1)
        _     <- TestClock.adjust(9.seconds)
        early <- store.moved
        _     <- TestClock.adjust(1.second)
        _     <- movedTimes(store, 2)
      yield assertTrue(early == 1)
    },
    test("a full call is repeated at once, without waiting for a tick") {
      for
        store <- InMemoryQueueStore.make
        _     <- store.movesWith(Right(QueueStore.Moved(100, 0, full = true, nextDueIn = None, failed = 0)))
        _     <- store.movesWith(Right(QueueStore.Moved(1, 0, full = false, nextDueIn = None, failed = 0)))
        _     <- moving(store)
        _     <- movedTimes(store, 2)
      yield assertCompletes
    },
    test("a failed call is logged, and the backstop's tick tries again") {
      for
        store <- InMemoryQueueStore.make
        _     <- store.movesWith(Left(Unreachable))
        _     <- moving(store)
        _     <- movedTimes(store, 1)
        _     <- TestClock.adjust(backstop)
        _     <- movedTimes(store, 2)
      yield assertCompletes
    },
    test("an unreachable part of the store does not stop the rest from arming the next tick") {
      for
        store <- InMemoryQueueStore.make
        _     <- store.movesWith(Right(QueueStore.Moved(0, 0, full = false, nextDueIn = Some(5.seconds), failed = 1)))
        _     <- moving(store)
        _     <- movedTimes(store, 1)
        _     <- TestClock.adjust(5.seconds)
        _     <- movedTimes(store, 2)
      yield assertCompletes
    },
  ) @@ TestAspect.timeout(30.seconds)

  private object Support {

    /** The timer's backstop in these tests, long enough that only a test that waits for it sees it fire. */
    val backstop: Duration = 1.minute

    /** A store failure, as an adapter would report one. */
    case object Unreachable extends ApplicationError.AdapterError:
      override def message: String = "unreachable"

    /**
     * A mover and its timer running over a fresh store with nothing queued up.
     *
     * @return the store
     */
    def started: UIO[InMemoryQueueStore] =
      InMemoryQueueStore.make.tap(moving)

    /**
     * Start a timer and a mover over the store.
     *
     * @param store the store the mover moves in
     * @return noop once both are forked
     */
    def moving(store: InMemoryQueueStore): UIO[Unit] =
      for
        timer <- DueTimer.make(DueTimer.Config(Duration.Zero, Duration.Zero, backstop))
        mover  = ScheduleMover(Monitor.Noop, store, timer, ScheduleMover.Config(limit = 100))
        _     <- timer.run.fork
        _     <- mover.run.fork
      yield ()

    /**
     * Wait until the store has been asked to move at least `count` times, or fail.
     *
     * @param store the store
     * @param count how many moves to wait for
     * @return noop once that many were asked for; fails, naming the shortfall, when they were not within the
     *         patience
     */
    def movedTimes(store: InMemoryQueueStore, count: Int): IO[String, Unit] =
      for
        reached <- Live.live(movedAtLeast(store, count).timeout(MaintenanceSpecSupport.patience))
        moved   <- store.moved
        _       <- ZIO.fail(s"expected $count moves, saw $moved").unless(reached.isDefined)
      yield ()

    /**
     * Look at the count, and look again after a yield while it is short.
     *
     * Unbounded on its own: [[movedTimes]] bounds it with the patience.
     *
     * @param store the store
     * @param count how many moves to wait for
     * @return noop once that many were asked for
     */
    def movedAtLeast(store: InMemoryQueueStore, count: Int): UIO[Unit] =
      for
        moved <- store.moved
        _     <- (ZIO.yieldNow *> movedAtLeast(store, count)).unless(moved >= count)
      yield ()
  }
