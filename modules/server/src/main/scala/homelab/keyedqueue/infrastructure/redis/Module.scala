package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
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
      Connection.make(Connection.Config(config.maxWait, config.redisUrl, config.cluster))
    }

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
  val store: ZLayer[Connection & Scripts & QueueConfig, ApplicationError, QueueStore] = ZLayer.scoped {
    for
      connection <- ZIO.service[Connection]
      scripts    <- ZIO.service[Scripts]
      config     <- ZIO.service[QueueConfig]
      waiters    <- Waiters.make
      listener   <- WakeListener.make(connection, waiters, config.wakeBuckets, config.wakeBlock)
      // Forked here rather than in the composition root because the store is unusable without it: a
      // consumer that finds nothing waits on a signal, and an unrun listener never raises one.
      _          <- listener.run.forkScoped
      store      <- RedisQueueStore.make(connection, scripts, waiters, config.leaseTtl, config.wakeBuckets)
    yield store
  }
