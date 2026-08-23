package homelab.keyedqueue.application.grpc


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.service.usecase.*
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis.{ ClaimerPool, Connection, Scripts, RedisQueueStore, Watchdog }
import homelab.keyedqueue.domain.types.WorkerId
import io.grpc.ServerBuilder
import scalapb.zio_grpc.{ ServerLayer, ServiceList }
import zio.*


/**
 * The composition root for the gRPC adapter: connections, claimers, the repair loop, and the server.
 *
 * '''Two kinds of connection.''' The claimers may block, and their number is the ceiling on concurrent
 * `Dequeue` calls; everything else — enqueue, settle, heartbeat, the sweeps — shares one connection that
 * must never park, because a blocking command there would stall every other caller on it.
 */
object GrpcApplication:

  /**
   * Build and run the service until interrupted.
   *
   * @param config where Redis is, how long leases last, how many claimers to open
   * @return never completes successfully; aborts with `QueueError` when the substrate cannot be set up
   */
  def serve(config: QueueConfig): ZIO[Scope, QueueError, Nothing] =
    for
      client   <- Connection.client(config.redisUrl)
      shared   <- Connection.open(client, config.maxWait)
      scripts  <- Scripts.load(shared)
      admin     = RedisQueueStore(shared, scripts, WorkerId("sweeper"), config.leaseTtl)
      claimers <- ClaimerPool.make(client, config)
      watchdog <- Watchdog.make(admin, config)
      // Claimers must stay registered even while idle: registration lapses on silence, and a claim made
      // after that would be born unrecoverable.
      _        <- claimers.beat.repeat(Schedule.fixed(Duration.fromMillis(config.leaseTtl.toMillis / 3))).forkScoped
      _        <- ZIO.logInfo(
                    s"dkq listening on ${config.port}, redis=${config.redisUrl}, " +
                      s"claimers=${config.claimers}, lease=${config.leaseTtl.toSeconds}s"
                  )
      _        <- server(config, service(admin, claimers, watchdog, config)).build
                    .mapError(error => QueueError.StoreUnavailable(s"the server did not start: ${error.getMessage}"))
      forever  <- ZIO.never
    yield forever

  /**
   * The service, with each use case pointed at the right kind of connection.
   *
   * @param admin the shared, never-blocking connection
   * @param claimers the pool that may block
   * @param watchdog told which queues to sweep
   * @param config the wait ceiling
   * @return the gRPC service
   */
  private def service(
    admin: RedisQueueStore,
    claimers: ClaimerPool,
    watchdog: Watchdog,
    config: QueueConfig,
  ): QueueService =
    QueueService(
      EnqueueUseCase(admin),
      DequeueUseCase((queue, wait) => claimers.borrow(_.claim(queue, wait)), config.maxWait),
      SettleUseCase(admin),
      HeartbeatUseCase(admin),
      watchdog,
    )

  /**
   * The server itself, for the life of the scope.
   *
   * @param config the port to listen on
   * @param service what to serve
   * @return the server layer
   */
  private def server(config: QueueConfig, service: QueueService) =
    ServerLayer.fromServiceList(
      ServerBuilder.forPort(config.port),
      ServiceList.add(service),
    )
