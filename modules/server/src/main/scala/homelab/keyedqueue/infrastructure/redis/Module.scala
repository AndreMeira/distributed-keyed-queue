package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis.keys.KeyLayout
import homelab.keyedqueue.infrastructure.redis.script.QueueScripts
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
      connection <- Connection.make(Connection.Config(config.maxWait, config.redisUrl, config.cluster), layout)
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
   * The queue itself, as the port.
   *
   * Fallible now that the listener resolves where each wake stream stands before its first read — a position
   * that cannot be read is a store that cannot be built.
   *
   * @return the layer
   */
  val stores: ZLayer[Connection & QueueScripts & QueueConfig & Monitor & KeyLayout, ApplicationError, QueueStore & LockStore] =
    ZLayer.scopedEnvironment {
      for
        monitor    <- ZIO.service[Monitor]
        connection <- ZIO.service[Connection]
        scripts    <- ZIO.service[QueueScripts]
        config     <- ZIO.service[QueueConfig]
        layout     <- ZIO.service[KeyLayout]
        // Before anything is built or served: an instance whose layout disagrees with the store's must not
        // come up at all — see KeyLayout.
        _          <- connection.provide(layout.verify)
        queueReady <- QueueReadiness.make
        lockReady  <- LockReadiness.make
        // One listener over the partition wake streams, routing each entry by the kind it carries — see
        // ReadinessListener. Queue entries wake `queueReady` (one token, one consumer); lock entries wake
        // `lockReady` (a broadcast — grants go by ticket, so every waiter must look).
        listener   <- ReadinessListener.make(connection, config.wakeBlock, layout, queueReady, lockReady)
        // Forked here rather than in the composition root because both stores are unusable without it: a
        // waiter that finds nothing parks on a readiness token, and an unrun listener offers none.
        _          <- listener.run.forkScoped
        queueStore <- RedisQueueStore.make(monitor, connection, scripts, queueReady, layout, config.leaseTtl)
        lockStore  <- connection.provide(RedisLockStore.make(monitor, connection, lockReady, layout))
      yield ZEnvironment[QueueStore](queueStore) ++ ZEnvironment[LockStore](lockStore)
    }
