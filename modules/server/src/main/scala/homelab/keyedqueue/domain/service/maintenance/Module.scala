package homelab.keyedqueue.domain.service.maintenance


import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.persistence.QueueStore
import zio.*


/** Wiring for maintenance. */
object Module:

  /**
   * The repair loop, running for the life of the scope.
   *
   * Scoped rather than plain, because building the watchdog forks the loop: a layer that handed one back
   * without starting it would give every caller something that looks like it is repairing and is not.
   *
   * @return the layer
   */
  val watchdog: ZLayer[QueueStore & Watchdog.Config, Nothing, Watchdog] = ZLayer.scoped:
    for
      store    <- ZIO.service[QueueStore]
      config   <- ZIO.service[Watchdog.Config]
      watchdog <- Watchdog.make(store, config)
    yield watchdog

  /**
   * The lock hygiene loop, running for the life of the scope.
   *
   * Scoped for the same reason as the watchdog: building it forks the loop.
   *
   * @return the layer
   */
  val lockCleanup: ZLayer[LockStore & LockCleanup.Config, Nothing, LockCleanup] = ZLayer.scoped:
    for
      store   <- ZIO.service[LockStore]
      config  <- ZIO.service[LockCleanup.Config]
      cleanup <- LockCleanup.make(store, config)
    yield cleanup
