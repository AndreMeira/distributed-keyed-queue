package homelab.keyedqueue.domain.service.maintenance


import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.persistence.QueueStore
import zio.*


/** Wiring for maintenance. */
object Module:

  /** Both repair loops, built and ready for [[init]] to start. */
  type Provided = Watchdog & LockCleanup

  /** The stores they repair, and the intervals they run on. */
  type Required = QueueStore & Watchdog.Config & LockStore & LockCleanup.Config

  /**
   * Start both repair loops, for the life of the caller's scope.
   *
   * Until this has run, nothing reclaims an abandoned claim or removes a lock hold left by a dead holder.
   *
   * @return noop once both loops are running
   */
  def init: ZIO[Watchdog & LockCleanup & Scope, Nothing, Unit] =
    for
      watchdog <- ZIO.service[Watchdog]
      cleanup  <- ZIO.service[LockCleanup]
      _        <- watchdog.run.forkScoped.interruptible
      _        <- cleanup.run.forkScoped.interruptible
    yield ()

  /**
   * Both loops, as one layer.
   *
   * @return the layer
   */
  lazy val layer: ZLayer[Required, Nothing, Provided] = watchdog ++ lockCleanup

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
