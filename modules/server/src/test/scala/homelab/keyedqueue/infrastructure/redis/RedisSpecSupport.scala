package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis.Container.Type
import homelab.keyedqueue.infrastructure.redis.Module as RedisModule
import homelab.keyedqueue.infrastructure.redis.keys.KeyLayout
import org.testcontainers.containers.GenericContainer
import zio.*
import zio.test.TestAspect


object RedisSpecSupport {

  opaque type Init = Unit

  lazy val layer: ZLayer[QueueConfig, ApplicationError, RedisModule.Provided] =
    monitor >+> RedisModule.layer

  /**
   * 
   */
  lazy val monitor: ULayer[Monitor] =
    ZLayer.succeed(Monitor.Noop)

  /**
   *
   */
  def config(lease: Duration): ZLayer[Any, ApplicationError, QueueConfig] =
    container >>> ZLayer.scoped:
      for container <- ZIO.service[Container.Type]
      yield Helper.config(Helper.redisUrl(container), leaseTtl = lease)

  /**
   *
   */
  lazy val container: ZLayer[Any, ApplicationError, Container.Type] =
    ZLayer.scoped(Container.run)

  object Aspect {

    /**
     * 
     */
    type InitDependency =
      Connection & KeyLayout & QueueConfig & QueueReadiness & LockReadiness & Scope

    /**
     * The wake path, running for exactly as long as the suite.
     */
    val init: TestAspect[Nothing, InitDependency, ApplicationError, Any] =
      TestAspect.beforeAll(RedisModule.init)
  }
}
