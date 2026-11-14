package homelab.keyedqueue.domain.service.maintenance


import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.service.persistence.LockStore
import homelab.keyedqueue.domain.service.persistence.QueueStore
import zio.*


/** Wiring for maintenance. */
object Module:

  /** The repair loops and the schedule mover with its timer, built and ready for [[init]] to start. */
  type Provided = Watchdog & LockCleanup & DueTimer & ScheduleMover

  /** The stores they work on, and the intervals they run on. */
  type Required =
    Monitor & QueueStore & Watchdog.Config & LockStore & LockCleanup.Config & DueTimer.Config & ScheduleMover.Config

  /**
   * Start the repair loops, the timer and the schedule mover, for the life of the caller's scope.
   *
   * Until this has run, nothing reclaims an abandoned claim, removes a lock hold left by a dead holder, or
   * moves a scheduled message that has fallen due.
   *
   * @return noop once every loop is running
   */
  def init: ZIO[Watchdog & LockCleanup & DueTimer & ScheduleMover & Scope, Nothing, Unit] =
    for
      watchdog <- ZIO.service[Watchdog]
      cleanup  <- ZIO.service[LockCleanup]
      timer    <- ZIO.service[DueTimer]
      mover    <- ZIO.service[ScheduleMover]
      _        <- watchdog.run.forkScoped.interruptible
      _        <- cleanup.run.forkScoped.interruptible
      _        <- timer.run.forkScoped.interruptible
      _        <- mover.run.forkScoped.interruptible
    yield ()

  /**
   * Every loop, as one layer.
   *
   * @return the layer
   */
  lazy val layer: ZLayer[Required, Nothing, Provided] = watchdog ++ lockCleanup ++ (dueTimer >+> scheduleMover)

  /**
   * The timer that wakes the schedule mover, built but not firing.
   *
   * @return the layer
   */
  val dueTimer: ZLayer[DueTimer.Config, Nothing, DueTimer] =
    ZLayer(ZIO.serviceWithZIO[DueTimer.Config](DueTimer.make))

  /**
   * The schedule mover, built but not running.
   *
   * @return the layer
   */
  val scheduleMover: ZLayer[Monitor & QueueStore & DueTimer & ScheduleMover.Config, Nothing, ScheduleMover] =
    ZLayer.fromFunction(ScheduleMover.apply)

  /**
   * The watchdog, built but not running.
   *
   * @return the layer
   */
  val watchdog: ZLayer[QueueStore & Watchdog.Config, Nothing, Watchdog] = ZLayer:
    for
      store    <- ZIO.service[QueueStore]
      config   <- ZIO.service[Watchdog.Config]
      watchdog <- Watchdog.make(store, config)
    yield watchdog

  /**
   * The lock hygiene loop, built but not running.
   *
   * @return the layer
   */
  val lockCleanup: ZLayer[LockStore & LockCleanup.Config, Nothing, LockCleanup] = ZLayer:
    for
      store  <- ZIO.service[LockStore]
      config <- ZIO.service[LockCleanup.Config]
    yield LockCleanup(store, config)
