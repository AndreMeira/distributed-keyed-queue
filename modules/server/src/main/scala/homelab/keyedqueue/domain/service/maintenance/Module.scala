package homelab.keyedqueue.domain.service.maintenance


import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.service.persistence.LockStore
import homelab.keyedqueue.domain.service.persistence.QueueStore
import zio.*


/** Wiring for maintenance. */
object Module:

  /** The repair loops, and the two movers with their timers, built and ready for [[init]] to start. */
  type Provided = Watchdog & LockCleanup & DueTimers & ScheduleMover & BackoffMover

  /** The stores they work on, and the intervals they run on. */
  type Required =
    Monitor & QueueStore & Watchdog.Config & LockStore & LockCleanup.Config & DueTimers.Config & ScheduleMover.Config & BackoffMover.Config

  /**
   * Start the repair loops, the two timers and the two movers, for the life of the caller's scope.
   *
   * Until this has run, nothing reclaims an abandoned claim, removes a lock hold left by a dead holder, moves a
   * scheduled message that has fallen due, or releases a key whose backoff has ended.
   *
   * @return noop once every loop is running
   */
  def init: ZIO[Watchdog & LockCleanup & DueTimers & ScheduleMover & BackoffMover & Scope, Nothing, Unit] =
    for
      watchdog  <- ZIO.service[Watchdog]
      cleanup   <- ZIO.service[LockCleanup]
      timers    <- ZIO.service[DueTimers]
      schedules <- ZIO.service[ScheduleMover]
      backoffs  <- ZIO.service[BackoffMover]
      _         <- watchdog.run.forkScoped.interruptible
      _         <- cleanup.run.forkScoped.interruptible
      _         <- timers.schedules.run.forkScoped.interruptible
      _         <- timers.backoffs.run.forkScoped.interruptible
      _         <- schedules.run.forkScoped.interruptible
      _         <- backoffs.run.forkScoped.interruptible
    yield ()

  /**
   * Every loop, as one layer.
   *
   * @return the layer
   */
  lazy val layer: ZLayer[Required, Nothing, Provided] =
    watchdog ++ lockCleanup ++ (dueTimers >+> (scheduleMover ++ backoffMover))

  /**
   * The two timers, one per mover, built but not firing.
   *
   * @return the layer
   */
  val dueTimers: ZLayer[DueTimers.Config, Nothing, DueTimers] = ZLayer:
    for
      config    <- ZIO.service[DueTimers.Config]
      schedules <- DueTimer.make(config.schedules)
      backoffs  <- DueTimer.make(config.backoffs)
    yield DueTimers(schedules, backoffs)

  /**
   * The schedule mover, on the schedule timer, built but not running.
   *
   * @return the layer
   */
  val scheduleMover: ZLayer[Monitor & QueueStore & DueTimers & ScheduleMover.Config, Nothing, ScheduleMover] =
    ZLayer.fromFunction: (monitor: Monitor, store: QueueStore, timers: DueTimers, config: ScheduleMover.Config) =>
      ScheduleMover(monitor, store, timers.schedules, config)

  /**
   * The backoff mover, on the backoff timer, built but not running.
   *
   * @return the layer
   */
  val backoffMover: ZLayer[Monitor & QueueStore & DueTimers & BackoffMover.Config, Nothing, BackoffMover] =
    ZLayer.fromFunction: (monitor: Monitor, store: QueueStore, timers: DueTimers, config: BackoffMover.Config) =>
      BackoffMover(monitor, store, timers.backoffs, config)

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
