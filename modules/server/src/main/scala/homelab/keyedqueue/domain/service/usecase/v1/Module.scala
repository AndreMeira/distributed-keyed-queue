package homelab.keyedqueue.domain.service.usecase.v1


import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import zio.ZLayer


/**
 * Wiring for the synchronous use cases.
 *
 * Everything it needs is a port or a domain value, so this layer says exactly what the domain depends on:
 * somewhere to keep the queue, something that repairs abandoned work, and the parse that turns a request
 * into something the store accepts. The service's limits are not among them any more — they belong to the
 * parse, which is what enforces them. No adapter appears here, which is the property worth keeping.
 */
object Module:

  /**
   * The four use cases, as one dependency for whatever adapter serves them.
   *
   * @return the layer
   */
  val useCases: ZLayer[QueueStore & Watchdog & QueueInputValidation, Nothing, SyncUseCases] =
    ZLayer.fromFunction: (store: QueueStore, watchdog: Watchdog, validation: QueueInputValidation) =>
      SyncUseCases(
        enqueue = EnqueueUseCase(store, watchdog, validation),
        dequeue = DequeueUseCase(store, watchdog, validation),
        settle = SettleUseCase(store, validation),
        heartbeat = HeartbeatUseCase(store),
      )
