package homelab.keyedqueue.application.grpc.v1


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.application.grpc.v1.errors.GrpcError
import homelab.keyedqueue.domain.service.usecase.lock.LockUseCases
import homelab.keyedqueue.domain.service.usecase.queue.QueueUseCases
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.v1.ZioKeyedLockService.GKeyedLock
import homelab.keyedqueue.v1.ZioKeyedQueueService.GKeyedQueue
import io.grpc.{ ServerBuilder, StatusException }
import scalapb.zio_grpc.{ RequestContext, ScopedServer, ServiceList }
import zio.{ Scope, ZIO, ZLayer }


/**
 * Wiring for the gRPC adapter.
 *
 * The service takes the use cases and nothing else — no store, no connection, no configuration — because a
 * protocol adapter that reached past them could hide a decision where a second adapter would not find it.
 */
object Module:

  /** The two services, which [[init]] registers with the server it binds. */
  type Provided = QueueService & LockService

  /** The use cases they serve, and what they measure against. */
  type Required = QueueUseCases & LockUseCases & Monitor

  /**
   * Bind the port and serve the two services until interrupted.
   *
   * Binding is explicit: the port opens when this runs.
   * The port is released when the scope closes.
   *
   * @return never completes; aborts with `StartupFailed` when the port cannot be bound
   */
  def init: ZIO[QueueService & LockService & QueueConfig & Scope, ApplicationError, Nothing] =
    for
      config  <- ZIO.service[QueueConfig]
      queue   <- ZIO.service[QueueService]
      lock    <- ZIO.service[LockService]
      _       <- ScopedServer
                   .fromServiceList(ServerBuilder.forPort(config.port), ServiceList.add(bindable(queue)).add(bindable(lock)))
                   .mapError(error => StartupFailed(s"the gRPC server did not start: ${error.getMessage}"))
      serving <- ZIO.never
    yield serving

  /**
   * The queue service as the server binds it, its failures turned into statuses once for every RPC.
   *
   * Typed with `RequestContext` because that is the shape zio-grpc can bind; the service ignores its context,
   * so it accepts any.
   *
   * @param service the service, failing with the domain's errors
   * @return the same service, failing with `StatusException`
   */
  private def bindable(service: QueueService): GKeyedQueue[RequestContext, StatusException] =
    service.mapError(GrpcError.fromApplicationError)

  /**
   * The lock service as the server binds it, its failures turned into statuses once for every RPC.
   *
   * @param service the service, failing with the domain's errors
   * @return the same service, failing with `StatusException`
   */
  private def bindable(service: LockService): GKeyedLock[RequestContext, StatusException] =
    service.mapError(GrpcError.fromApplicationError)

  /**
   * Both services, as one layer.
   *
   * @return the layer
   */
  lazy val layer: ZLayer[Required, Nothing, Provided] = service ++ lockService

  /**
   * The service, over the synchronous use cases.
   *
   * @return the layer
   */
  val service: ZLayer[QueueUseCases & Monitor, Nothing, QueueService] =
    ZLayer.fromFunction: (monitor: Monitor, useCases: QueueUseCases) =>
      QueueService(monitor, useCases)

  /**
   * The lock service, over the lock store.
   *
   * @return the layer
   */
  val lockService: ZLayer[LockUseCases & Monitor, Nothing, LockService] =
    ZLayer.fromFunction: (monitor: Monitor, useCases: LockUseCases) =>
      LockService(monitor, useCases)
