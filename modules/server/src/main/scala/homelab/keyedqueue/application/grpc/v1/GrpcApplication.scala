package homelab.keyedqueue.application.grpc.v1


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.service.usecase.v1 as usecase
import homelab.keyedqueue.domain.service.validation as validation
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis as redis
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
   * @return never completes successfully; aborts when the substrate or the server cannot be set up
   */
  val serve: ZIO[QueueConfig, QueueError, Nothing] =
    (ZIO.service[Server] *> ZIO.never).provideSome[QueueConfig](
      redis.Module.connection,
      redis.Module.scripts,
      redis.Module.store,
      homelab.keyedqueue.domain.service.maintenance.Module.watchdog,
      homelab.keyedqueue.infrastructure.configuration.Module.syncUseCases,
      homelab.keyedqueue.infrastructure.configuration.Module.watchdog,
      validation.Module.input,
      usecase.Module.useCases,
      Module.service,
      Module.server,
    )
