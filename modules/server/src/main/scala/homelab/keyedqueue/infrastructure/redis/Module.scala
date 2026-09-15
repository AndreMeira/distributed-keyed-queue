package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis.keys.KeyLayout
import homelab.keyedqueue.infrastructure.redis.script.{ LockScripts, QueueScripts }
import zio.*


/**
 * Wiring for the Redis adapter.
 *
 * '''The connection split is the thing to notice.''' One shared connection serves everything that must never
 * park, and a pool of connections serves the one operation that must — so a idle claim cannot stall an
 * enqueue. Both live behind the port below, which is why the layer hands back `QueueStore` rather than the
 * implementation.
 */
object Module:

  /**
   * The ports, and the pieces [[init]] works on.
   *
   * '''Wider than the ports on purpose.''' Verifying the layout and starting the wake path are done by
   * `init` against the very instances the stores hold, so the connection, the layout and the two
   * readinesses have to be reachable from the composition root rather than hidden inside this module.
   */
  type Provided = QueueStore & LockStore & Connection & KeyLayout & QueueReadiness & LockReadiness

  /** Where Redis is and how it is sized, and what every call is traced against. */
  type Required = QueueConfig & Monitor

  /**
   * Everything this adapter has to do before it can serve, and nothing it can hold.
   *
   * '''The layers below build values; this does the work.''' Checking the store's layout, and starting the
   * readers that keep the readinesses fed, are effects with an order and a moment — not things a layer
   * should perform as a side effect of being asked for a value.
   *
   * The readers are forked into the caller's scope rather than a layer's, so they live exactly as long as
   * the effect that started them.
   *
   * @return noop once the layout is verified and the wake path is running; aborts with `Misconfigured` when
   *         the store was written under a different layout, and with `RedisFailure` when it cannot be read
   */
  def init: ZIO[
    Connection & KeyLayout & QueueConfig & QueueReadiness & LockReadiness & Scope,
    ApplicationError,
    Unit,
  ] =
    for
      connection <- ZIO.service[Connection]
      layout     <- ZIO.service[KeyLayout]
      config     <- ZIO.service[QueueConfig]
      queueReady <- ZIO.service[QueueReadiness]
      lockReady  <- ZIO.service[LockReadiness]
      // Before anything is served: an instance whose layout disagrees with the store's must not come up at
      // all — see KeyLayout.
      _          <- connection.provide(layout.verify)
      // The wake path, in two halves: `WakeConsumer` reads every partition's stream and hands on what
      // accumulated, `ReadinessProcessor` routes each entry by the kind it carries. Queue entries wake the
      // queue's readiness (one token, one consumer); lock entries wake the lock's (a broadcast — grants go
      // by ticket, so every waiter must look).
      consumer   <- WakeConsumer.make(connection, layout, config.wakeBlock)
      _          <- consumer.reachable
      _          <- consumer.positioned
      _          <- consumer.start.forkScoped
      _          <- ReadinessProcessor(consumer, queueReady, lockReady).run.forkScoped
    yield ()

  /**
   * Everything this adapter supplies, as one layer.
   *
   * @return the layer
   */
  lazy val layer: ZLayer[Required, ApplicationError, Provided] =
    ZLayer.makeSome[Required, Provided](
      layout,
      connection,
      scripts,
      lockScripts,
      readiness,
      stores,
    )

  /**
   * Where a caller waits for something to become claimable, or a lock to come free.
   *
   * One of each per instance, shared by the stores that park on them and by the processor that wakes them —
   * which is the whole reason they are a layer rather than something a store makes for itself.
   *
   * @return the layer
   */
  val readiness: ZLayer[Any, Nothing, QueueReadiness & LockReadiness] = ZLayer.fromZIOEnvironment:
    for
      queueReady <- QueueReadiness.make
      lockReady  <- LockReadiness.make
    yield ZEnvironment[QueueReadiness](queueReady) ++ ZEnvironment[LockReadiness](lockReady)

  /**
   * The lock's scripts, registered at startup so a missing or unparseable one fails here rather than on the
   * first acquire.
   *
   * @return the layer
   */
  val lockScripts: ZLayer[Connection, ApplicationError, LockScripts] =
    ZLayer(ZIO.serviceWithZIO[Connection](_.provide(LockScripts.make)))

  /**
   * The connections: one shared, and `claimers` more that may be occupied.
   *
   * '''The client is built here rather than layered separately, because which client to build is a runtime
   * question.''' Lettuce has no URL scheme that tells a cluster from a single server, so `cluster` in the
   * configuration says which — and a layer cannot choose its own inputs, so the choice has to happen inside
   * one. Everything downstream sees a [[Connection]] either way.
   *
   * @return the layer
   */
  val connection: ZLayer[QueueConfig & KeyLayout, ApplicationError, Connection] = ZLayer.scoped:
    for
      config     <- ZIO.service[QueueConfig]
      layout     <- ZIO.service[KeyLayout]
      conf        = Connection.Config(config.maxWait, config.redisUrl, config.cluster)
      connection <- Connection.make(conf, layout)
    yield connection

  /**
   * How this deployment divides its keys.
   *
   * A layer rather than a value passed around, so that everything which names a key or opens a connection
   * for one takes it from the same place — and a layer rather than a constant because the count is the
   * deployment's: a cluster spreads across all of them, a single server uses one.
   *
   * @return the layer
   */
  val layout: ZLayer[QueueConfig, Nothing, KeyLayout] = ZLayer:
    for config <- ZIO.service[QueueConfig]
    yield if config.cluster then KeyLayout.cluster else KeyLayout.single

  /**
   * The scripts, registered at startup so a missing or unparseable one fails here rather than on the first
   * message.
   *
   * @return the layer
   */
  val scripts: ZLayer[Connection, ApplicationError, QueueScripts] =
    ZLayer(ZIO.serviceWithZIO[Connection](_.provide(QueueScripts.make)))

  /**
   * Both stores, as their ports.
   *
   * Construction only: everything the two need was built by the layers above, and everything that had to
   * happen before they can be used happens in [[init]].
   *
   * @return the layer
   */
  val stores: ZLayer[
    Monitor & Connection & QueueScripts & LockScripts & QueueConfig & KeyLayout & QueueReadiness & LockReadiness,
    Nothing,
    QueueStore & LockStore,
  ] = ZLayer.fromZIOEnvironment:
    for
      monitor     <- ZIO.service[Monitor]
      connection  <- ZIO.service[Connection]
      scripts     <- ZIO.service[QueueScripts]
      lockScripts <- ZIO.service[LockScripts]
      config      <- ZIO.service[QueueConfig]
      layout      <- ZIO.service[KeyLayout]
      queueReady  <- ZIO.service[QueueReadiness]
      lockReady   <- ZIO.service[LockReadiness]
      queueStore   = RedisQueueStore(monitor, connection, scripts, queueReady, layout, config.leaseTtl)
      lockStore    = RedisLockStore(monitor, connection, lockScripts, lockReady, layout)
    yield ZEnvironment[QueueStore](queueStore) ++ ZEnvironment[LockStore](lockStore)
