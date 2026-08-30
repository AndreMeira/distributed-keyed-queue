package homelab.keyedqueue.infrastructure.configuration


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.usecase.v1.SyncUseCases
import zio.ZLayer


/**
 * Wiring for configuration.
 *
 * Several layers rather than one, because the domain must not see [[QueueConfig]]: this is where a file full
 * of infrastructure settings is narrowed to the handful of rules each domain service actually needs.
 */
object Module:

  /**
   * The whole configuration, read from `resources/config/queue.conf`.
   *
   * @return the layer; fails startup when the file is missing or invalid
   */
  val config: ZLayer[Any, QueueError, QueueConfig] = ZLayer(QueueConfig.load)

  /**
   * The slice of it the domain is allowed to know.
   *
   * @return the layer
   */
  val syncUseCases: ZLayer[QueueConfig, Nothing, SyncUseCases.Config] =
    ZLayer.fromFunction((config: QueueConfig) => SyncUseCases.Config(config.maxWait))

  /**
   * The slice of it the repair loop is allowed to know.
   *
   * @return the layer
   */
  val watchdog: ZLayer[QueueConfig, Nothing, Watchdog.Config] =
    ZLayer.fromFunction((config: QueueConfig) => Watchdog.Config(config.sweepInterval, config.sweepLimit))
