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
     * Wait until the store has been asked to move at least `count` times.
     *
     * @param store the store
     * @param count how many moves to wait for
     * @return noop once that many were asked for; fails, naming the shortfall, when they were not within the
     *         patience
     */
    def movedTimes(store: InMemoryQueueStore, count: Int): IO[String, Unit] =
      movedWithin(store, count, MaintenanceSpecSupport.patience)

    /**
     * Look at the count, and look again after a yield while it is short and patience is left.
     *
     * @param store the store
     * @param count how many moves to wait for
     * @param left how many more yields the wait may take
     * @return noop once that many were asked for; fails when the patience runs out first
     */
    def movedWithin(store: InMemoryQueueStore, count: Int, left: Int): IO[String, Unit] =
      store.moved.flatMap(counted(store, count, left))

    /**
     * What a look at the count found, and what follows.
     *
     * @param store the store
     * @param count how many moves to wait for
     * @param left how many more yields the wait may take
     * @param moved how many moves were asked for so far
     * @return noop when enough were; another look after a yield; or a failure naming the shortfall
     */
    def counted(store: InMemoryQueueStore, count: Int, left: Int)(moved: Int): IO[String, Unit] =
      if moved >= count then ZIO.unit
      else if left > 0 then ZIO.yieldNow *> movedWithin(store, count, left - 1)
      else ZIO.fail(s"expected $count moves, saw $moved")
  }
