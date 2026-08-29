package homelab.keyedqueue.domain.service.usecase.v1


import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import zio.ZLayer


/**
 * Wiring for the synchronous apply cases.
 *
 * Everything it needs is a port or a domain value, so this layer says exactly what the domain depends on:
 * somewhere to keep the queue, something that repairs abandoned work, what makes a request valid, and
 * the limits to enforce. No adapter appears here, which is the property worth keeping.
 */
object Module:

  /**
   * The four apply cases, as one dependency for whatever adapter serves them.
   *
   * @return the layer
   */
  val useCases: ZLayer[QueueStore & Watchdog & QueueInputValidation & SyncUseCases.Config, Nothing, SyncUseCases] =
    ZLayer.fromFunction: (store: QueueStore, watchdog: Watchdog, validation: QueueInputValidation, config: SyncUseCases.Config) =>
      SyncUseCases(
        enqueue = EnqueueUseCase(store, watchdog, validation),
        dequeue = DequeueUseCase(store, watchdog, validation, config.maxWait),
        settle = SettleUseCase(store),
        heartbeat = HeartbeatUseCase(store),
      )
