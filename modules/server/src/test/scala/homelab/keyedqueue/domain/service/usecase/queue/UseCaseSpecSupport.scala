package homelab.keyedqueue.domain.service.usecase.queue


import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.readiness.QueueReadiness
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import zio.*


/**
 * What a queue use-case spec needs: the use case, and the two handles a test drives it through.
 *
 * The store is a [[InMemoryQueueStore]] and the readiness is the real one, so a spec over this layer
 * exercises the waiting itself and reaches no substrate.
 */
object UseCaseSpecSupport {

  /** The limits a parsed demand is held to, wide enough that these specs are never clamped. */
  val limits: QueueInputValidation.Config = QueueInputValidation.Config(maxWait = 30.seconds, maxBatchLimit = 32)

  /** How often the watchdog would sweep, which these specs never let it do. */
  val sweeping: Watchdog.Config = Watchdog.Config(sweepInterval = 1.minute, sweepLimit = 100)

  /**
   * The use case, the store that answers it, and the readiness it waits on.
   *
   * Built per test rather than shared: a store holds the grants a test gave it and a readiness holds that
   * queue's token, so tests that shared them would read each other's state.
   *
   * @return the layer
   */
  lazy val layer: ZLayer[Any, Nothing, DequeueUseCase & InMemoryQueueStore & QueueReadiness] =
    ZLayer.fromZIOEnvironment:
      for
        store     <- InMemoryQueueStore.make
        readiness <- QueueReadiness.make
        watchdog  <- Watchdog.make(store, sweeping)
        dequeue    = DequeueUseCase(store, watchdog, QueueInputValidation(limits), readiness)
      yield ZEnvironment[DequeueUseCase](dequeue) ++
        ZEnvironment[InMemoryQueueStore](store) ++
        ZEnvironment[QueueReadiness](readiness)
}
