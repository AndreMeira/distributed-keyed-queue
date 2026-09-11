package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.infrastructure.configuration.{ Misconfigured, QueueConfig }
import org.testcontainers.containers.GenericContainer
import zio.*
import zio.test.*


/**
 * The boot-time layout check, against real Valkey.
 *
 * The three paths that matter: a first boot records and proceeds, a matching boot proceeds, and a
 * mismatched boot refuses with both numbers in the message — before anything could be served against the
 * wrong layout. `accept` is the deliberate override, so after it the once-mismatched count is the
 * recorded one.
 */
object LayoutSpec extends ZIOSpecDefault:

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
      yield config(url, buckets = 1)

  /** The config as an instance with this bucket count would read it. */
  private def config(url: String, buckets: Int): QueueConfig =
    QueueConfig(url, cluster = false, 0, 30.seconds, 1.second, 100, 120.seconds, 10.minutes, 10.minutes, 200.millis, buckets, 5.seconds, 32)

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

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Layout")(
    test("a first boot records the layout, and a matching boot passes ever after") {
      for
        conf  <- ZIO.service[QueueConfig]
        _     <- boot(conf)(Layout.verify(conf))
        again <- boot(conf)(Layout.verify(conf)).exit
      yield assertTrue(again.isSuccess)
    },
    test("a mismatched boot refuses, naming both numbers, and serves nothing") {
      for
        conf    <- ZIO.service[QueueConfig]
        _       <- boot(conf)(Layout.verify(conf))
        other    = config(conf.redisUrl, buckets = 4)
        refused <- boot(other)(Layout.verify(other)).exit
      yield assertTrue(refused.causeOption.flatMap(_.failureOption).exists {
        case Misconfigured(reason) => reason.contains("1") && reason.contains("4") && reason.contains("layout accept")
        case _                     => false
      })
    },
    test("accept overrides deliberately: the once-refused layout is then the recorded one") {
      for
        conf     <- ZIO.service[QueueConfig]
        _        <- boot(conf)(Layout.verify(conf))
        other     = config(conf.redisUrl, buckets = 4)
        _        <- boot(other)(Layout.accept(other))
        accepted <- boot(other)(Layout.verify(other)).exit
        original <- boot(conf)(Layout.verify(conf)).exit
      yield assertTrue(accepted.isSuccess, original.isFailure)
    },
  ).provideSomeShared[Scope](substrate) @@ TestAspect.sequential @@ TestAspect.timeout(3.minutes)
