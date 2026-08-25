package homelab.keyedqueue.application.grpc.v1


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.service.usecase.v1.SyncUseCases
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import io.grpc.ServerBuilder
import scalapb.zio_grpc.{ Server, ServerLayer, ServiceList }
import zio.ZLayer


/**
 * Wiring for the gRPC adapter.
 *
 * The service takes the use cases and nothing else — no store, no connection, no configuration — because a
 * protocol adapter that reached past them could hide a decision where a second adapter would not find it.
 */
object Module:

  /**
   * The service, over the synchronous use cases.
   *
   * @return the layer
   */
  val service: ZLayer[SyncUseCases, Nothing, QueueService] =
    ZLayer.fromFunction: (useCases: SyncUseCases) =>
      QueueService(useCases.enqueue, useCases.dequeue, useCases.settle, useCases.heartbeat)

  /**
   * The server, started when the layer is built and shut down when the scope closes.
   *
   * Its failure is narrowed to `QueueError` so the whole graph fails with one type: a port already taken is
   * the same kind of event as a Redis that will not answer — the process cannot start, and nothing about it
   * is worth retrying in place.
   *
   * @return the layer
   */
  val server: ZLayer[QueueService & QueueConfig, QueueError, Server] =
    ZLayer
      .service[QueueConfig]
      .flatMap: environment =>
        ServerLayer.fromServiceList(
          ServerBuilder.forPort(environment.get[QueueConfig].port),
          ServiceList.addFromEnvironment[QueueService],
        )
      .mapError(error => QueueError.StartupFailed(s"the gRPC server did not start: ${error.getMessage}"))
