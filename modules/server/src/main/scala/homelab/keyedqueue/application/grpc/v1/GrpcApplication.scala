package homelab.keyedqueue.application.grpc.v1


import homelab.common.error.ApplicationError
import homelab.keyedqueue.application.grpc.v1.Module as GrpcModule
import homelab.keyedqueue.domain.service.maintenance.Module as MaintenanceModule
import homelab.keyedqueue.domain.service.readiness.Module as ReadinessModule
import homelab.keyedqueue.domain.service.usecase.Module as UseCaseModule
import homelab.keyedqueue.domain.service.validation.Module as ValidationModule
import homelab.keyedqueue.infrastructure.configuration.Module as ConfigurationModule
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.tracing.Module as TracingModule
import homelab.keyedqueue.infrastructure.redis.Module as RedisModule
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
   * The three `init`s run in order: the store's layout is verified and the wake path started, then the
   * repair loops, then the server — which never returns, so it is last.
   *
   * @param conf where Redis is, what to listen on, and the sizes every module reads its own slice of
   * @return never completes successfully; aborts when the substrate or the server cannot be set up
   */
  def serve(conf: QueueConfig): ZIO[Scope, ApplicationError, Nothing] =
    (
      ZIO.unit
        *> RedisModule.init
        *> ReadinessModule.init
        *> MaintenanceModule.init
        *> GrpcModule.init
    ).provideSome[Scope](
      GrpcModule.layer,
      ReadinessModule.layer,
      UseCaseModule.layer,
      ValidationModule.layer,
      MaintenanceModule.layer,
      RedisModule.layer,
      TracingModule.layer,
      ConfigurationModule.layer,
      ZLayer.succeed(conf),
    )
