package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import io.lettuce.core.api.sync.RedisCommands
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
  val connection: ZLayer[QueueConfig, ApplicationError, Connection] = ZLayer.scoped:
    ZIO.service[QueueConfig].flatMap { config =>
      Connection.make(Connection.Config(config.maxWait, config.redisUrl, config.cluster), wakeStreams)
    }

  /**
   * Every stream a listener blocks on: the queue's partitions, and the lock's.
   *
   * Named once because two things must agree on it — the connections opened for blocking reads, and the
   * routes those reads are announced through. A stream in one and not the other is a wake nobody hears.
   */
  private val wakeStreams: Chunk[RedisKey] = QueueKeys.wakeStreams.toChunk :+ LockKeys.wake

  /**
   * The scripts, registered at startup so a missing or unparseable one fails here rather than on the first
   * message.
   *
   * @return the layer
   */
  val scripts: ZLayer[Connection, ApplicationError, Scripts] =
    ZLayer(ZIO.serviceWithZIO[Connection](_.provide(Scripts.make)))

  /**
   * The queue itself, as the port.
   *
   * Fallible now that the listener resolves where each wake stream stands before its first read — a position
   * that cannot be read is a store that cannot be built.
   *
   * @return the layer
   */
  val stores: ZLayer[Connection & Scripts & QueueConfig & Monitor, ApplicationError, QueueStore & LockStore] =
    ZLayer.scopedEnvironment {
      for
        monitor    <- ZIO.service[Monitor]
        connection <- ZIO.service[Connection]
        scripts    <- ZIO.service[Scripts]
        config     <- ZIO.service[QueueConfig]
        // Before anything is built or served: an instance whose layout disagrees with the store's must not
        // come up at all — see KeyLayout.
        _          <- connection.provide(KeyLayout.verify)
        queueReady <- Readiness.make
        lockReady  <- Broadcast.make
        // One listener over both stores' wake streams, routing each to its own readiness — see WakeListener.
        // The queue's partition streams wake `queueReady` (one token, one consumer); the lock's one
        // stream wakes `lockReady` (a broadcast — grants go by ticket, so every waiter must look).
        routes      = QueueKeys.wakeStreams.toChunk.map(_ -> queueReady).toMap + (LockKeys.wake -> lockReady)
        listener   <- WakeListener.make(connection, config.wakeBlock, routes)
        // Forked here rather than in the composition root because both stores are unusable without it: a
        // waiter that finds nothing parks on a readiness token, and an unrun listener offers none.
        _          <- listener.run.forkScoped
        queueStore <- RedisQueueStore.make(monitor, connection, scripts, queueReady, config.leaseTtl)
        lockStore  <- connection.provide(RedisLockStore.make(monitor, connection, lockReady))
      yield ZEnvironment[QueueStore](queueStore) ++ ZEnvironment[LockStore](lockStore)
    }
