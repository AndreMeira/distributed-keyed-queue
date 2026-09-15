package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis.keys.KeyLayout
import org.testcontainers.containers.GenericContainer
import zio.*
import zio.test.*


/**
 * What a spec needs to run against a real Valkey: a container, a configuration, and instances over it.
 *
 * An instance here is what the composition root builds — [[Module.layer]] for the values, [[Module.init]]
 * for the work — so two of them over one container are two independent pods, and a step added to the
 * adapter's startup is inherited by every spec that builds one.
 */
object RedisSpecSupport:

  /** How these specs divide their keys: one partition, as a single server does. */
  val layout: KeyLayout = KeyLayout.single

  /**
   * A Valkey for the suite, and the configuration that reaches it.
   *
   * @param leaseTtl how long a claim is held before it can be reclaimed
   * @param port where the gRPC server binds, for the specs that start one
   * @return the layer; the container stops when the scope it was built in closes
   */
  def substrate(leaseTtl: Duration = 30.seconds, port: Int = 0): ZLayer[Any, Throwable, QueueConfig] =
    ZLayer.scoped:
      for container <- ZIO.acquireRelease(started)(stopped)
      yield config(url(container), leaseTtl, port)

  /**
   * One instance of the adapter over the configured store, built and started in the caller's scope.
   *
   * @param configured where Redis is and how this instance is sized
   * @return both stores, the connection, the layout and the two readinesses; aborts with `Misconfigured`
   *         when the store was written under another layout, and with `RedisFailure` when it cannot be read
   */
  def instance(configured: QueueConfig): ZIO[Scope, ApplicationError, ZEnvironment[Module.Provided]] =
    for
      provided <- Module.layer.build.provideSome[Scope](ZLayer.succeed(configured), monitor)
      _        <- Module.init.provideSome[Scope](ZLayer.succeedEnvironment(provided), ZLayer.succeed(configured))
    yield provided

  /**
   * A connection to the suite's store, with nothing started over it.
   *
   * @param configured where Redis is, and how long a caller may wait for a connection
   * @return the connection; aborts with `RedisFailure` when the store cannot be reached
   */
  def connection(configured: QueueConfig): ZIO[Scope, RedisFailure, Connection] =
    Connection.make(Connection.Config(configured.maxWait, configured.redisUrl, configured.cluster), layout)

  /**
   * What these specs trace against.
   *
   * Unobserved: they are about what the adapter does to Redis, and `Noop` keeps the telemetry wiring out
   * of the assertions while leaving every code path as it is.
   *
   * @return the layer
   */
  private def monitor: ULayer[Monitor] = ZLayer.succeed[Monitor](Monitor.Noop)

  /**
   * The values a spec runs with, named — only the two a spec ever varies are its own.
   *
   * @param redis where the container answers
   * @param leaseTtl how long a claim is held before it can be reclaimed
   * @param port where the gRPC server binds
   * @return the configuration
   */
  private def config(redis: String, leaseTtl: Duration, port: Int): QueueConfig =
    QueueConfig(
      redisUrl = redis,
      cluster = false,
      port = port,
      leaseTtl = leaseTtl,
      sweepInterval = 1.second,
      sweepLimit = 100,
      lockTrimInterval = 120.seconds,
      lockTrimGrace = 10.minutes,
      lockMaxTtl = 10.minutes,
      wakeBlock = 200.millis,
      maxWait = 5.seconds,
      maxBatchLimit = 32,
    )

  /**
   * Start a Valkey for the suite.
   *
   * @return the started container; aborts with whatever the Docker client refused with
   */
  private def started: Task[GenericContainer[?]] =
    ZIO.attemptBlocking:
      // Docker Engine 29 rejects the API version docker-java negotiates by default with an HTTP 400;
      // pinning it is what the toolkit's Testcontainers specs do too.
      val _                              = java.lang.System.setProperty("api.version", "1.40")
      // GenericContainer is self-referentially generic (SELF extends GenericContainer), which Scala infers
      // as Nothing — hence the explicit wildcard and no chaining.
      val container: GenericContainer[?] = GenericContainer("valkey/valkey:8.1-alpine")
      container.setExposedPorts(java.util.List.of(Integer.valueOf(6379)))
      container.start()
      container

  /**
   * Stop a container, whatever became of the suite that used it.
   *
   * @param container the container to stop
   * @return noop
   */
  private def stopped(container: GenericContainer[?]): UIO[Unit] =
    ZIO.attemptBlocking(container.stop()).ignore

  /**
   * Where a started container answers.
   *
   * @param container the started container
   * @return the URL of its mapped Redis port
   */
  private def url(container: GenericContainer[?]): String =
    s"redis://${container.getHost}:${container.getMappedPort(6379)}"
