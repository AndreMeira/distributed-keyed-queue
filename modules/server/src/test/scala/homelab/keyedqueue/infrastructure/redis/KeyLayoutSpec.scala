package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.infrastructure.configuration.{ Misconfigured, QueueConfig }
import homelab.keyedqueue.infrastructure.redis.keys.KeyLayout
import org.testcontainers.containers.GenericContainer
import zio.*
import zio.test.*


/**
 * The boot-time schema check, against real Valkey.
 *
 * The three paths that matter: a first boot records and proceeds, a matching boot proceeds, and a
 * mismatched boot refuses with both versions in the message — before anything could be served against
 * structures the code would misread. `accept` is the deliberate override, gated on the store being
 * drained.
 */
object KeyLayoutSpec extends ZIOSpecDefault:

  /** A Valkey container for the suite, and the config to reach it. */
  private val substrate: ZLayer[Any, Any, QueueConfig] =
    ZLayer.scoped:
      for
        container <- ZIO.acquireRelease(
                       ZIO.attemptBlocking:
                         val _                            = java.lang.System.setProperty("api.version", "1.40")
                         val started: GenericContainer[?] = GenericContainer("valkey/valkey:8.1-alpine")
                         started.setExposedPorts(java.util.List.of(Integer.valueOf(6379)))
                         started.start()
                         started
                     )(container => ZIO.attemptBlocking(container.stop()).ignore)
        url        = s"redis://${container.getHost}:${container.getMappedPort(6379)}"
      yield config(url)

  /** The config to reach the suite's store. */
  private def config(url: String): QueueConfig =
    QueueConfig(url, cluster = false, 0, 30.seconds, 1.second, 100, 120.seconds, 10.minutes, 10.minutes, 200.millis, 5.seconds, 32)

  /**
   * Run a layout effect the way boot does: on a fresh connection to the suite's store.
   *
   * @param configured the instance's configuration
   * @param effect what to run against the store
   * @return what the effect returns
   */
  private def boot[A](
    configured: QueueConfig
  )(
    effect: ZIO[Connection.Commands, Any, A]
  ): ZIO[Scope, Any, A] =
    Connection
      .make(
        Connection.Config(configured.maxWait, configured.redisUrl, configured.cluster),
        KeyLayout.of(cluster = false),
      )
      .flatMap(_.provide(effect))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("KeyLayout")(
    test("a first boot records the layout, and a matching boot passes ever after") {
      for
        conf  <- ZIO.service[QueueConfig]
        _     <- boot(conf)(KeyLayout.of(conf.cluster).verify)
        again <- boot(conf)(KeyLayout.of(conf.cluster).verify).exit
      yield assertTrue(again.isSuccess)
    },
    test("a schema the code does not expect refuses the boot, and accept records the code's own") {
      // The store claims a schema this code never wrote — the shape of an incompatible predecessor. The
      // boot must refuse before touching structures it would misread, and accept re-stamps the code's version.
      val predecessor = Connection.use: redis =>
        ZIO.attemptBlocking(redis.set("dkq:layout:schema", "999".getBytes)).unit
      for
        conf     <- ZIO.service[QueueConfig]
        _        <- boot(conf)(predecessor)
        refused  <- boot(conf)(KeyLayout.of(conf.cluster).verify).exit
        _        <- boot(conf)(KeyLayout.of(conf.cluster).accept)
        restored <- boot(conf)(KeyLayout.of(conf.cluster).verify).exit
      yield assertTrue(
        refused.causeOption.flatMap(_.failureOption).exists {
          case Misconfigured(reason) => reason.contains("schema") && reason.contains("999")
          case _                     => false
        },
        restored.isSuccess,
      )
    },
  ).provideSomeShared[Scope](substrate) @@ TestAspect.sequential @@ TestAspect.timeout(3.minutes)
