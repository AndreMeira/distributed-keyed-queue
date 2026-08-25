package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.service.maintenance.Watchdog
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
 * park, and a pool of connections serves the one operation that must — so a blocking claim cannot stall an
 * enqueue. Both live behind the ports below, which is why the layers hand back `QueueStore` and `Watchdog`
 * rather than their implementations.
 */
object Module:

  /** The type the shared connection is published as — Lettuce's synchronous API over text keys and raw values. */
  private type Commands = RedisCommands[String, Array[Byte]]

  /**
   * The client, shut down with the scope.
   *
   * @return the layer
   */
  val client: ZLayer[QueueConfig, QueueError, RedisClient] =
    ZLayer.scoped(ZIO.serviceWithZIO[QueueConfig](config => Connection.client(config.redisUrl)))

  /**
   * The shared connection: everything except claiming runs on it, so nothing that blocks may.
   *
   * @return the layer
   */
  val shared: ZLayer[RedisClient & QueueConfig, QueueError, Commands] =
    ZLayer.scoped(
      ZIO.serviceWithZIO[RedisClient](client => ZIO.serviceWithZIO[QueueConfig](config => Connection.open(client, config.maxWait)))
    )

  /**
   * The scripts, registered at startup so a missing or unparseable one fails here rather than on the first
   * message.
   *
   * @return the layer
   */
  val scripts: ZLayer[Commands, QueueError, Scripts] =
    ZLayer(ZIO.serviceWithZIO[Commands](Scripts.make))

  /**
   * The blocking connections, kept registered for as long as they exist.
   *
   * The beat is forked here rather than in the composition root because it is a property of the pool: a
   * claimer that stops announcing itself becomes unrecoverable, whether or not anything else is running.
   *
   * @return the layer
   */
  val claimers: ZLayer[RedisClient & QueueConfig, QueueError, ClaimerPool] =
    ZLayer.scoped(
      for
        client <- ZIO.service[RedisClient]
        config <- ZIO.service[QueueConfig]
        pool   <- ClaimerPool.make(client, config)
        _      <- pool.beat.repeat(Schedule.fixed(Duration.fromMillis(config.leaseTtl.toMillis / 3))).forkScoped
      yield pool
    )

  /**
   * The queue itself, as the port.
   *
   * @return the layer
   */
  val store: ZLayer[Commands & Scripts & ClaimerPool & QueueConfig, Nothing, QueueStore] =
    ZLayer.fromFunction: (redis: Commands, scripts: Scripts, claimers: ClaimerPool, config: QueueConfig) =>
      RedisQueueStore(redis, scripts, claimers, WorkerId("shared"), config.leaseTtl)

  /**
   * The repair loop, running for the life of the scope.
   *
   * @return the layer
   */
  val watchdog: ZLayer[QueueStore & QueueConfig, Nothing, Watchdog] =
    ZLayer.scoped(
      ZIO.serviceWithZIO[QueueStore](store => ZIO.serviceWithZIO[QueueConfig](config => RedisWatchdog.make(store, config)))
    )
