package homelab.keyedqueue.domain.service.maintenance


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.usecase.queue.InMemoryQueueStore
import zio.*
import zio.test.*


/**
 * The backoff mover over an in-memory store whose answers a test queues up, and a real timer on the `TestClock`:
 * when it releases, and what it asks the timer for afterwards. Its shape is the schedule mover's.
 */
object BackoffMoverSpec extends ZIOSpecDefault:
  import Support.*

  def spec = suite("BackoffMover")(
    test("it releases once as soon as it starts, on the timer's boot tick") {
      for
        store <- InMemoryQueueStore.make
        _     <- releasing(store)
        _     <- releasedTimes(store, 1)
      yield assertCompletes
    },
    test("it asks the timer for a tick when the store says the next backoff ends") {
      for
        store <- InMemoryQueueStore.make
        _     <- store.releasesWith(Right(QueueStore.Released(0, full = false, nextDueIn = Some(10.seconds), failed = 0)))
        _     <- releasing(store)
        _     <- releasedTimes(store, 1)
        _     <- TestClock.adjust(9.seconds)
        early <- store.released
        _     <- TestClock.adjust(1.second)
        _     <- releasedTimes(store, 2)
      yield assertTrue(early == 1)
    },
    test("a full call is repeated at once, without waiting for a tick") {
      for
        store <- InMemoryQueueStore.make
        _     <- store.releasesWith(Right(QueueStore.Released(100, full = true, nextDueIn = None, failed = 0)))
        _     <- store.releasesWith(Right(QueueStore.Released(1, full = false, nextDueIn = None, failed = 0)))
        _     <- releasing(store)
        _     <- releasedTimes(store, 2)
      yield assertCompletes
    },
    test("a failed call is logged, and the backstop's tick tries again") {
      for
        store <- InMemoryQueueStore.make
        _     <- store.releasesWith(Left(Unreachable))
        _     <- releasing(store)
        _     <- releasedTimes(store, 1)
        _     <- TestClock.adjust(backstop)
        _     <- releasedTimes(store, 2)
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
     * Start a timer and a backoff mover over the store.
     *
     * @param store the store the mover releases in
     * @return noop once both are forked
     */
    def releasing(store: InMemoryQueueStore): UIO[Unit] =
      for
        timer <- DueTimer.make(DueTimer.Config(Duration.Zero, Duration.Zero, backstop))
        mover  = BackoffMover(Monitor.Noop, store, timer, BackoffMover.Config(limit = 100))
        _     <- timer.run.fork
        _     <- mover.run.fork
      yield ()

    /**
     * Wait until the store has been asked to release at least `count` times, or fail.
     *
     * @param store the store
     * @param count how many releases to wait for
     * @return noop once that many were asked for; fails, naming the shortfall, when they were not within the
     *         patience
     */
    def releasedTimes(store: InMemoryQueueStore, count: Int): IO[String, Unit] =
      releasedWithin(store, count, MaintenanceSpecSupport.patience)

    /**
     * Look at the count, and look again after a yield while it is short and patience is left.
     *
     * @param store the store
     * @param count how many releases to wait for
     * @param left how many more yields the wait may take
     * @return noop once that many were asked for; fails when the patience runs out first
     */
    def releasedWithin(store: InMemoryQueueStore, count: Int, left: Int): IO[String, Unit] =
      store.released.flatMap(counted(store, count, left))

    /**
     * What a look at the count found, and what follows.
     *
     * @param store the store
     * @param count how many releases to wait for
     * @param left how many more yields the wait may take
     * @param released how many releases were asked for so far
     * @return noop when enough were; another look after a yield; or a failure naming the shortfall
     */
    def counted(store: InMemoryQueueStore, count: Int, left: Int)(released: Int): IO[String, Unit] =
      if released >= count then ZIO.unit
      else if left > 0 then ZIO.yieldNow *> releasedWithin(store, count, left - 1)
      else ZIO.fail(s"expected $count releases, saw $released")
  }
