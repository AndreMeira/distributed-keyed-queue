package homelab.keyedqueue.application.grpc


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.usecase.*
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.serialisation.EnvelopeCodec
import homelab.keyedqueue.infrastructure.codecs.storage.ProtobufEnvelopeCodec
import homelab.keyedqueue.infrastructure.redis.{ ClaimerPool, Connection, RedisQueueStore, RedisWatchdog, Scripts }
import homelab.keyedqueue.domain.types.WorkerId
import io.grpc.ServerBuilder
import scalapb.zio_grpc.{ Server, ServerLayer, ServiceList }
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
      claimers <- ClaimerPool.make(client, config)
      store     = RedisQueueStore(shared, scripts, claimers, WorkerId("shared"), config.leaseTtl)
      watchdog <- RedisWatchdog.make(store, config)
      // Claimers must stay registered even while idle: registration lapses on silence, and a claim made
      // after that would be born unrecoverable.
      _        <- claimers.beat.repeat(Schedule.fixed(Duration.fromMillis(config.leaseTtl.toMillis / 3))).forkScoped
      _        <- ZIO.logInfo(
                    s"dkq listening on ${config.port}, redis=${config.redisUrl}, " +
                      s"claimers=${config.claimers}, lease=${config.leaseTtl.toSeconds}s"
                  )
      _        <- server(config, service(store, watchdog, config)).build
                    .mapError(error => QueueError.StoreUnavailable(s"the server did not start: ${error.getMessage}"))
      forever  <- ZIO.never
    yield forever

  /**
   * The service, over one store.
   *
   * Every use case holds the same port. That the claim happens on a borrowed connection and the rest on a
   * shared one is the store's business, not theirs.
   *
   * @param store the queue
   * @param watchdog told which queues to sweep
   * @param config the wait ceiling
   * @return the gRPC service
   */
  private def service(store: QueueStore, watchdog: Watchdog, config: QueueConfig): QueueService =
    val codec: EnvelopeCodec = ProtobufEnvelopeCodec()
    QueueService(
      EnqueueUseCase(store, codec, watchdog),
      DequeueUseCase(store, codec, watchdog, config.maxWait),
      SettleUseCase(store),
      HeartbeatUseCase(store),
    )

  /**
   * The server itself, for the life of the scope.
   *
   * @param config the port to listen on
   * @param service what to serve
   * @return the server as a layer, started when built and shut down when the scope closes
   */
  private def server(config: QueueConfig, service: QueueService): ZLayer[Any, Throwable, Server] =
    ServerLayer.fromServiceList(
      ServerBuilder.forPort(config.port),
      ServiceList.add(service),
    )
