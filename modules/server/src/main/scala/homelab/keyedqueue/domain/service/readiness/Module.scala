package homelab.keyedqueue.domain.service.readiness


import homelab.common.error.ApplicationError
import homelab.common.messaging.Consumer
import zio.*


/**
 * Wiring for waiting: the two readinesses, and the routing that keeps them fed.
 *
 * Nothing here names a substrate. What reads the wakes is a [[Consumer.Batched]] of [[Wake]]s, which an
 * adapter supplies — see `docs/architecture/readiness-and-wake.md`.
 */
object Module:

  /** Where a consumer waits for work, and where a lock's waiters park. */
  type Provided = QueueReadiness & LockReadiness

  /** Where wakes come from, whichever substrate reads them. */
  type Required = Consumer.Batched[ApplicationError.AdapterError, Wake]

  /**
   * Start routing wakes into the two readinesses.
   *
   * Interruptible, so the routing stops with the scope that started it however that scope is entered.
   *
   * @return noop once the routing is running; aborts with `AdapterError` when the wake path fails
   */
  def init: ZIO[Provided & Required & Scope, ApplicationError, Unit] =
    for
      wakes      <- ZIO.service[Consumer.Batched[ApplicationError.AdapterError, Wake]]
      queueReady <- ZIO.service[QueueReadiness]
      lockReady  <- ZIO.service[LockReadiness]
      _          <- ReadinessProcessor(wakes, queueReady, lockReady).run.forkScoped.interruptible
    yield ()

  /**
   * Both readinesses, as one layer.
   *
   * @return the layer
   */
  lazy val layer: ZLayer[Any, Nothing, Provided] = readiness

  /**
   * One of each per instance, shared by everything that parks on them and by the routing that wakes them.
   *
   * @return the layer
   */
  val readiness: ZLayer[Any, Nothing, QueueReadiness & LockReadiness] = ZLayer.fromZIOEnvironment:
    for
      queueReady <- QueueReadiness.make
      lockReady  <- LockReadiness.make
    yield ZEnvironment[QueueReadiness](queueReady) ++ ZEnvironment[LockReadiness](lockReady)
