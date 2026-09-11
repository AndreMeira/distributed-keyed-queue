package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.infrastructure.configuration.{ Misconfigured, QueueConfig }
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
  private def boot[A](configured: QueueConfig)(
    effect: ZIO[Connection.Commands, Any, A]
  ): ZIO[Scope, Any, A] =
    Connection
      .make(Connection.Config(configured.maxWait, configured.redisUrl, configured.cluster))
      .flatMap(_.provide(effect))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("KeyLayout")(
    test("a first boot records the layout, and a matching boot passes ever after") {
      for
        conf  <- ZIO.service[QueueConfig]
        _     <- boot(conf)(KeyLayout.verify)
        again <- boot(conf)(KeyLayout.verify).exit
      yield assertTrue(again.isSuccess)
    },
    test("accept verifies the drain of dkq's own keys, and ignores the store's other tenants") {
      // The store may be a Redis that already existed: a leftover dkq key refuses the accept naming it,
      // while a foreign key is none of dkq's business — the accept succeeds right past it.
      val plant  = Connection.use: redis =>
        ZIO.attemptBlocking {
          redis.set("{w:3}:q:jobs:ready", "left-behind".getBytes)
          redis.set("other-app:cache", "not-ours".getBytes)
        }.unit
      val uproot = Connection.use: redis =>
        ZIO.attemptBlocking(redis.del("{w:3}:q:jobs:ready")).unit
      for
        conf    <- ZIO.service[QueueConfig]
        _       <- boot(conf)(plant)
        refused <- boot(conf)(KeyLayout.accept).exit
        _       <- boot(conf)(uproot)
        shared  <- boot(conf)(KeyLayout.accept).exit
      yield assertTrue(
        refused.causeOption.flatMap(_.failureOption).exists {
          case Misconfigured(reason) => reason.contains("drained") && reason.contains("{w:3}:q:jobs:ready")
          case _                     => false
        },
        shared.isSuccess, // the foreign key is still in the store
      )
    },
    test("a schema the code does not expect refuses the boot, and accept records the code's own") {
      // The store claims a schema this code never wrote — the shape of an incompatible predecessor. The
      // boot must refuse before touching structures it would misread, and accept (drained: marker only)
      // re-stamps the code's version.
      val predecessor = Connection.use: redis =>
        ZIO.attemptBlocking(redis.set("dkq:layout:schema", "999".getBytes)).unit
      for
        conf     <- ZIO.service[QueueConfig]
        _        <- boot(conf)(predecessor)
        refused  <- boot(conf)(KeyLayout.verify).exit
        _        <- boot(conf)(KeyLayout.accept)
        restored <- boot(conf)(KeyLayout.verify).exit
      yield assertTrue(
        refused.causeOption.flatMap(_.failureOption).exists {
          case Misconfigured(reason) => reason.contains("schema") && reason.contains("999")
          case _                     => false
        },
        restored.isSuccess,
      )
    },
  ).provideSomeShared[Scope](substrate) @@ TestAspect.sequential @@ TestAspect.timeout(3.minutes)
