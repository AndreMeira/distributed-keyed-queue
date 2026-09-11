package homelab.keyedqueue.infrastructure.configuration


import homelab.keyedqueue.domain.service.maintenance.{ LockCleanup, Watchdog }
import homelab.keyedqueue.domain.service.validation.{ LockInputValidation, QueueInputValidation }
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
   * The lock validator's limit, read from the same config the queue's `max_wait` comes from — one ceiling
   * for how long any call is allowed to wait.
   *
   * @return the layer
   */
  val lockValidation: ZLayer[QueueConfig, Nothing, LockInputValidation.Config] =
    ZLayer.fromFunction((config: QueueConfig) => LockInputValidation.Config(config.maxWait))

  /**
   * The slice of it the repair loop is allowed to know.
   *
   * @return the layer
   */
  val watchdog: ZLayer[QueueConfig, Nothing, Watchdog.Config] =
    ZLayer.fromFunction((config: QueueConfig) => Watchdog.Config(config.sweepInterval, config.sweepLimit))

  /**
   * The lock hygiene loop's slice: its own cadence and grace, the sweep's batch bound.
   *
   * @return the layer
   */
  val lockCleanup: ZLayer[QueueConfig, Nothing, LockCleanup.Config] =
    ZLayer.fromFunction((config: QueueConfig) => LockCleanup.Config(config.lockTrimInterval, config.lockTrimGrace, config.sweepLimit))
