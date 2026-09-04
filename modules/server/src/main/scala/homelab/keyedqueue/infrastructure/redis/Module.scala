package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
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
  val connection: ZLayer[QueueConfig, QueueError, Connection] = ZLayer.scoped:
    ZIO.service[QueueConfig].flatMap { config =>
      Connection.pool(Connection.Config(config.maxWait, config.redisUrl, config.cluster))
    }

  /**
   * The scripts, registered at startup so a missing or unparseable one fails here rather than on the first
   * message.
   *
   * @return the layer
   */
  val scripts: ZLayer[Connection, QueueError, Scripts] =
    ZLayer(ZIO.serviceWithZIO[Connection](_.provide(Scripts.make)))

  /**
   * The queue itself, as the port.
   *
   * @return the layer
   */
  val store: ZLayer[Connection & Scripts & QueueConfig, Nothing, QueueStore] = ZLayer.scoped {
    for
      connection <- ZIO.service[Connection]
      scripts    <- ZIO.service[Scripts]
      config     <- ZIO.service[QueueConfig]
      waiters    <- Waiters.make
      listener   <- WakeListener.make(connection, waiters, config.wakeBlock)
      // Forked here rather than in the composition root because the store is unusable without it: a
      // consumer that finds nothing waits on the doorbell, and an unrun listener never rings it.
      _          <- listener.run.forkScoped
      store      <- RedisQueueStore.make(connection, scripts, listener, config.leaseTtl)
    yield store
  }
