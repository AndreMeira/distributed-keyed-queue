package homelab.keyedqueue.application.grpc.v1


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.service.usecase.v1.SyncLockUseCases
import homelab.keyedqueue.domain.service.usecase.v1.SyncUseCases
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import io.grpc.ServerBuilder
import scalapb.zio_grpc.{ Server, ServerLayer, ServiceList }
import zio.{ URIO, ZIO, ZLayer }


/**
 * Wiring for the gRPC adapter.
 *
 * The service takes the use cases and nothing else — no store, no connection, no configuration — because a
 * protocol adapter that reached past them could hide a decision where a second adapter would not find it.
 */
object Module:

  /** The running server, which [[init]] holds open. */
  type Provided = Server

  /** The use cases it serves, what it measures against, and the port it listens on. */
  type Required = SyncUseCases & SyncLockUseCases & Monitor & QueueConfig

  /**
   * Hold the server open until interrupted.
   *
   * '''Asking for the [[Server]] is not decoration.''' A layer graph builds only what the effect requires
   * and `ZIO.never` requires nothing, so without this the whole stack would be constructed lazily — which
   * is to say never — and the process would sit there serving no one.
   *
   * @return never completes
   */
  def init: URIO[Server, Nothing] = ZIO.service[Server] *> ZIO.never

  /**
   * The services and the server they are registered with, as one layer.
   *
   * @return the layer
   */
  lazy val layer: ZLayer[Required, ApplicationError, Provided] =
    (service ++ lockService) >>> server

  /**
   * The service, over the synchronous use cases.
   *
   * @return the layer
   */
  val service: ZLayer[SyncUseCases & Monitor, Nothing, QueueService] =
    ZLayer.fromFunction: (monitor: Monitor, useCases: SyncUseCases) =>
      QueueService(monitor, useCases)

  /**
   * The lock service, over the lock store.
   *
   * @return the layer
   */
  val lockService: ZLayer[SyncLockUseCases & Monitor, Nothing, LockService] =
    ZLayer.fromFunction: (monitor: Monitor, useCases: SyncLockUseCases) =>
      LockService(monitor, useCases)

  /**
   * The server, started when the layer is built and shut down when the scope closes.
   *
   * Its failure is narrowed to `ApplicationError` so the whole graph fails with one type: a port already taken is
   * the same kind of event as a Redis that will not answer — the process cannot start, and nothing about it
   * is worth retrying in place.
   *
   * @return the layer
   */
  val server: ZLayer[QueueService & LockService & QueueConfig, ApplicationError, Server] =
    ZLayer
      .service[QueueConfig]
      .flatMap: environment =>
        ServerLayer.fromServiceList(
          ServerBuilder.forPort(environment.get[QueueConfig].port),
          ServiceList.addFromEnvironment[QueueService].addFromEnvironment[LockService],
        )
      .mapError(error => StartupFailed(s"the gRPC server did not start: ${error.getMessage}"))
