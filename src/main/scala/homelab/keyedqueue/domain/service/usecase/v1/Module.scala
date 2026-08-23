package homelab.keyedqueue.domain.service.usecase.v1


import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.serialisation.EnvelopeCodec
import zio.ZLayer


/**
 * Wiring for the synchronous use cases.
 *
 * Everything it needs is a port or a domain value, so this layer says exactly what the domain depends on:
 * somewhere to keep the queue, a way to serialise a message, something that repairs abandoned work, and the
 * limits to enforce. No adapter appears here, which is the property worth keeping.
 */
object Module:

  /**
   * The four use cases, as one dependency for whatever adapter serves them.
   *
   * @return the layer
   */
  val useCases: ZLayer[QueueStore & EnvelopeCodec & Watchdog & SyncUseCases.Config, Nothing, SyncUseCases] =
    ZLayer.fromFunction:
      (store: QueueStore, codec: EnvelopeCodec, watchdog: Watchdog, config: SyncUseCases.Config) =>
        SyncUseCases(
          enqueue = EnqueueUseCase(store, codec, watchdog),
        dequeue = DequeueUseCase(store, codec, watchdog, config.maxWait),
        settle = SettleUseCase(store),
        heartbeat = HeartbeatUseCase(store),
      )
