package homelab.keyedqueue.application.grpc.v1


import homelab.common.error.ApplicationError
import homelab.keyedqueue.application.grpc.v1.Module as GrpcModule
import homelab.keyedqueue.domain.service.maintenance.Module as MaintenanceModule
import homelab.keyedqueue.domain.service.usecase.v1.Module as UseCaseModule
import homelab.keyedqueue.domain.service.validation.Module as ValidationModule
import homelab.keyedqueue.infrastructure.configuration.Module as ConfigurationModule
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis.Module as RedisModule
import scalapb.zio_grpc.Server
import zio.*


/**
 * The composition root for the gRPC adapter.
 *
 * It names the modules and nothing else: what each layer needs is declared by the layer, so adding a
 * dependency is an edit in the module that owns it rather than here. The configuration is deliberately left
 * as a requirement — the service reads it from a file, a test provides one for a container it just started,
 * and neither has to know how the other does it.
 */
object GrpcApplication:

  /**
   * Serve until interrupted.
   *
   * Asking for the [[Server]] is not decoration: a layer graph builds only what the effect requires, and
   * `ZIO.never` requires nothing — so without this the whole stack would be constructed lazily, which is to
   * say never, and the process would sit there serving no one.
   *
   * @param conf where Redis is, what to listen on, and the sizes every module reads its own slice of
   * @return never completes successfully; aborts when the substrate or the server cannot be set up
   */
  def serve(conf: QueueConfig): ZIO[Any, ApplicationError, Nothing] =
    (ZIO.service[Server] *> ZIO.never).provide(
      ZLayer.succeed(conf),
      RedisModule.connection,
      RedisModule.scripts,
      RedisModule.store,
      MaintenanceModule.watchdog,
      ConfigurationModule.validation,
      ConfigurationModule.watchdog,
      ValidationModule.input,
      UseCaseModule.useCases,
      GrpcModule.service,
      GrpcModule.server,
    )
