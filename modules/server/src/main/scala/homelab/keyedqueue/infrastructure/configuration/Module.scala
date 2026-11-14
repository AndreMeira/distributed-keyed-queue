package homelab.keyedqueue.infrastructure.configuration


import homelab.keyedqueue.domain.service.maintenance.{ BackoffMover, DueTimer, DueTimers, LockCleanup, ScheduleMover, Watchdog }
import homelab.keyedqueue.domain.service.validation.{ LockInputValidation, QueueInputValidation }
import zio.ZLayer


/**
 * Wiring for configuration.
 *
 * Several layers rather than one, because the domain must not see [[QueueConfig]]: this is where a file full
 * of infrastructure settings is narrowed to the handful of rules each domain service actually needs.
 */
object Module:

  /** Every slice of the configuration the modules downstream read. */
  type Provided = QueueInputValidation.Config & LockInputValidation.Config & Watchdog.Config & LockCleanup.Config & DueTimers.Config &
    ScheduleMover.Config & BackoffMover.Config

  /** The configuration itself, which the composition root supplies. */
  type Required = QueueConfig

  /**
   * Every slice, as one layer.
   *
   * @return the layer
   */
  lazy val layer: ZLayer[Required, Nothing, Provided] =
    validation ++ lockValidation ++ watchdog ++ lockCleanup ++ dueTimers ++ scheduleMover ++ backoffMover

  /**
   * The slice of it the parse is allowed to know.
   *
   * @return the layer
   */
  val validation: ZLayer[QueueConfig, Nothing, QueueInputValidation.Config] =
    ZLayer.fromFunction((config: QueueConfig) =>
      QueueInputValidation.Config(config.maxWait, config.maxBatchLimit, config.maxScheduleAhead, config.maxRetryAfter)
    )

  /**
   * The lock validator's limit, read from the same config the queue's `max_wait` comes from — one ceiling
   * for how long any call is allowed to wait.
   *
   * @return the layer
   */
  val lockValidation: ZLayer[QueueConfig, Nothing, LockInputValidation.Config] =
    ZLayer.fromFunction((config: QueueConfig) => LockInputValidation.Config(config.maxWait, config.lockMaxTtl))

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

  /**
   * The two timers' slices: each one's quantum, slack and backstop.
   *
   * @return the layer
   */
  val dueTimers: ZLayer[QueueConfig, Nothing, DueTimers.Config] =
    ZLayer.fromFunction((config: QueueConfig) =>
      DueTimers.Config(
        schedules = DueTimer.Config(config.scheduleQuantum, config.scheduleSlack, config.scheduleBackstop),
        backoffs = DueTimer.Config(config.backoffQuantum, config.backoffSlack, config.backoffBackstop),
      )
    )

  /**
   * The backoff mover's slice: how much one call releases.
   *
   * @return the layer
   */
  val backoffMover: ZLayer[QueueConfig, Nothing, BackoffMover.Config] =
    ZLayer.fromFunction((config: QueueConfig) => BackoffMover.Config(config.backoffLimit))

  /**
   * The schedule mover's slice: how much one call moves.
   *
   * @return the layer
   */
  val scheduleMover: ZLayer[QueueConfig, Nothing, ScheduleMover.Config] =
    ZLayer.fromFunction((config: QueueConfig) => ScheduleMover.Config(config.scheduleLimit))
