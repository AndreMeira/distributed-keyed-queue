package homelab.keyedqueue.infrastructure.configuration


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.service.usecase.v1.SyncUseCases
import zio.ZLayer


/**
 * Wiring for configuration.
 *
 * Two layers rather than one, because the domain must not see [[QueueConfig]]: this is where a file full of
 * infrastructure settings is narrowed to the handful of rules the apply cases actually enforce.
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
