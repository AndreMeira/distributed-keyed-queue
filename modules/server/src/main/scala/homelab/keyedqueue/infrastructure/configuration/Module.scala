package homelab.keyedqueue.infrastructure.configuration


import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import zio.ZLayer


/**
 * Wiring for configuration.
 *
 * Several layers rather than one, because the domain must not see [[QueueConfig]]: this is where a file full
 * of infrastructure settings is narrowed to the handful of rules each domain service actually needs.
 */
object Module:

  /**
   * The slice of it the parse is allowed to know.
   *
   * @return the layer
   */
  val validation: ZLayer[QueueConfig, Nothing, QueueInputValidation.Config] =
    ZLayer.fromFunction((config: QueueConfig) => QueueInputValidation.Config(config.maxWait, config.maxBatchLimit))

  /**
   * The slice of it the repair loop is allowed to know.
   *
   * @return the layer
   */
  val watchdog: ZLayer[QueueConfig, Nothing, Watchdog.Config] =
    ZLayer.fromFunction((config: QueueConfig) => Watchdog.Config(config.sweepInterval, config.sweepLimit))
