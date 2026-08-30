package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.WorkerId
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import io.lettuce.core.RedisClient
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
   * The client, shut down with the scope.
   *
   * @return the layer
   */
  val client: ZLayer[QueueConfig, QueueError, RedisClient] = ZLayer.scoped:
    ZIO.service[QueueConfig].flatMap(config => Connection.client(config.redisUrl))

  /**
   * The connections: one shared, and `claimers` more that may be occupied.
   *
   * @return the layer
   */
  val connection: ZLayer[RedisClient & QueueConfig, QueueError, Connection] = ZLayer.scoped:
    for
      client <- ZIO.service[RedisClient]
      config <- ZIO.service[QueueConfig]
      pool   <- Connection.pool(client, config.maxWait, config.maxWait + 10.seconds, config.claimers)
    yield pool

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
      store      <- RedisQueueStore.make(connection, scripts, WorkerId("shared"), config.leaseTtl)
    yield store
  }
