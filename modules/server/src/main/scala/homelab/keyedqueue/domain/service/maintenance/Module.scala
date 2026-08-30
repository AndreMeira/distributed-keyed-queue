package homelab.keyedqueue.domain.service.maintenance


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
