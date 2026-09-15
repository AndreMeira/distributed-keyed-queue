package homelab.keyedqueue.application.grpc.v1


import homelab.common.error.ApplicationError
import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis.{Container, RedisSpecSupport}
import homelab.keyedqueue.v1.ZioKeyedLockService.KeyedLockClient
import homelab.keyedqueue.v1.ZioKeyedQueueService.KeyedQueueClient
import io.grpc.ManagedChannelBuilder
import scalapb.zio_grpc.ZManagedChannel
import zio.*
import zio.test.TestAspect


object GrpcSpecSupport {

  /** Where this suite's server binds, and where its clients dial. */
  val port: Int = 19_099

  /**
   * Both clients pointed at [[port]] — one server, two APIs.
   */
  lazy val layer: ZLayer[Any, Throwable, KeyedQueueClient & KeyedLockClient] =
    ZLayer.scopedEnvironment:
      for
        queue <- KeyedQueueClient.scoped(channel)
        lock  <- KeyedLockClient.scoped(channel)
      yield ZEnvironment[KeyedQueueClient](queue) ++ ZEnvironment[KeyedLockClient](lock)

  /**
   * A Valkey of this suite's own, and the configuration that serves on [[port]].
   */
  lazy val config: ZLayer[Any, ApplicationError, QueueConfig] =
    RedisSpecSupport.container >>> ZLayer.scoped:
      for container <- ZIO.service[Container.Type]
      yield Helper.config(Helper.redisUrl(container), port = port)

  /**
   * A channel to this suite's server.
   *
   * @return the channel, closed with the scope it is opened in
   */
  private def channel: ZManagedChannel =
    ZManagedChannel(ManagedChannelBuilder.forAddress("localhost", port).usePlaintext())

  object Aspect {

    /**
     * What the server needs to come up: where Redis is, and a scope to run in.
     */
    type InitDependency = QueueConfig & Scope

    /**
     * The whole service, serving for exactly as long as the suite.
     */
    val init: TestAspect[Nothing, InitDependency, ApplicationError, Any] =
      TestAspect.beforeAll(serving)

    /**
     * Start the service and wait for the port to answer.
     *
     * Interruptible because `beforeAll` runs its effect as an acquire, and a fiber forked in an
     * uninterruptible region cannot be stopped when the suite ends.
     *
     * @return noop once the server is bound; aborts with `StartupFailed` when the port cannot be opened
     */
    private def serving: ZIO[InitDependency, ApplicationError, Unit] =
      for
        config <- ZIO.service[QueueConfig]
        _      <- GrpcApplication.serve(config).interruptible.forkScoped
        _      <- ZIO.sleep(1.second) // let the server bind before the client dials
      yield ()
  }
}
