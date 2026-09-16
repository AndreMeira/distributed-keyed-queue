package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.common.messaging.Consumer
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.readiness.Wake
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis.keys.KeyLayout
import homelab.keyedqueue.infrastructure.redis.script.{ LockScripts, QueueScripts }
import zio.*


/**
 * Wiring for the Redis adapter.
 *
 * One shared connection serves everything that must never
 * park, and a pool of connections serves the one operation that must — so an idle claim cannot stall an enqueue.
 */
object Module:

  /**
   * The ports, and the pieces [[init]] acts on: the connection it verifies over, the layout it checks, and
   * the two readinesses the wake path feeds.
   * Everything this module openly provides
   */
  type Provided = QueueStore & LockStore & Connection & KeyLayout & WakeConsumer & Consumer.Batched[ApplicationError.AdapterError, Wake]

  /**
   * Everything this module needs.
   */
  type Required = QueueConfig & Monitor

  /**
   * Check the store's layout and start the wake path, for the life of the caller's scope.
   *
   * Until this has run, the readinesses are never woken: a consumer parked on one waits out its patience,
   * and a lock waiter its recheck.
   *
   * @return noop once the layout is verified and the wake path is running; aborts with `Misconfigured` when
   *         the store was written under a different layout, and with `RedisFailure` when it cannot be read
   */
  def init: ZIO[Connection & KeyLayout & WakeConsumer & Scope, ApplicationError, Unit] =
    for
      connection <- ZIO.service[Connection]
      layout     <- ZIO.service[KeyLayout]
      consumer   <- ZIO.service[WakeConsumer]
      // Before anything is served: an instance whose layout disagrees with the store's must not come up at
      // all — see KeyLayout.
      _          <- connection.provide(layout.verify)
      // The reading half of the wake path; the routing half is the readiness module's.
      _          <- consumer.reachable
      _          <- consumer.positioned
      _          <- consumer.start.forkScoped.interruptible
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
      wakes,
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
  val wakes: ZLayer[
    Connection & KeyLayout & QueueConfig,
    ApplicationError,
    WakeConsumer & Consumer.Batched[ApplicationError.AdapterError, Wake],
  ] = ZLayer.scopedEnvironment:
    for
      connection <- ZIO.service[Connection]
      layout     <- ZIO.service[KeyLayout]
      config     <- ZIO.service[QueueConfig]
      consumer   <- WakeConsumer.make(connection, layout, config.wakeBlock)
    yield ZEnvironment[WakeConsumer](consumer) ++
      ZEnvironment[Consumer.Batched[ApplicationError.AdapterError, Wake]](consumer)

  /**
   * The lock's scripts, registered at startup so a missing
   * or unparseable one fails here rather than on the first acquire.
   *
   * @return the layer
   */
  val lockScripts: ZLayer[Connection, ApplicationError, LockScripts] =
    ZLayer(ZIO.serviceWithZIO[Connection](_.provide(LockScripts.make)))

  /**
   * The connections: one shared, and `claimers` more that may be occupied.
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
   * @return the layer
   */
  val layout: ZLayer[QueueConfig, Nothing, KeyLayout] = ZLayer:
    for config <- ZIO.service[QueueConfig]
    yield if config.cluster then KeyLayout.cluster else KeyLayout.single

  /**
   * The scripts, registered at startup so a missing
   * or unparseable one fails here rather than on the first message.
   *
   * @return the layer
   */
  val scripts: ZLayer[Connection, ApplicationError, QueueScripts] =
    ZLayer(ZIO.serviceWithZIO[Connection](_.provide(QueueScripts.make)))

  /**
   * Both stores, as their ports.
   *
   * @return the layer
   */
  val stores: ZLayer[
    Monitor & Connection & QueueScripts & LockScripts & QueueConfig & KeyLayout,
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
      queueStore   = RedisQueueStore(monitor, connection, scripts, layout, config.leaseTtl)
      lockStore    = RedisLockStore(monitor, connection, lockScripts, layout)
    yield ZEnvironment[QueueStore](queueStore) ++ ZEnvironment[LockStore](lockStore)
