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

  def spec: Spec[TestEnvironment & Scope, Any] = suite("KeyLayout")(
    test("a first boot records the layout, and a matching boot passes ever after") {
      for
        conf  <- ZIO.service[QueueConfig]
        _     <- boot(conf)(KeyLayout.verify(conf))
        again <- boot(conf)(KeyLayout.verify(conf)).exit
      yield assertTrue(again.isSuccess)
    },
    test("a mismatched boot refuses, naming both numbers, and serves nothing") {
      for
        conf    <- ZIO.service[QueueConfig]
        _       <- boot(conf)(KeyLayout.verify(conf))
        other    = config(conf.redisUrl, buckets = 4)
        refused <- boot(other)(KeyLayout.verify(other)).exit
      yield assertTrue(refused.causeOption.flatMap(_.failureOption).exists {
        case Misconfigured(reason) => reason.contains("1") && reason.contains("4") && reason.contains("layout accept")
        case _                     => false
      })
    },
    test("accept overrides deliberately: the once-refused layout is then the recorded one") {
      for
        conf     <- ZIO.service[QueueConfig]
        _        <- boot(conf)(KeyLayout.verify(conf))
        other     = config(conf.redisUrl, buckets = 4)
        _        <- boot(other)(KeyLayout.accept(other))
        accepted <- boot(other)(KeyLayout.verify(other)).exit
        original <- boot(conf)(KeyLayout.verify(conf)).exit
      yield assertTrue(accepted.isSuccess, original.isFailure)
    },
    test("accept verifies the drain: any key beyond the markers refuses it, an emptied store permits it") {
      val plant  = Connection.use: redis =>
        ZIO.attemptBlocking(redis.set("{q:0}:ready", "left-behind".getBytes)).unit
      val uproot = Connection.use: redis =>
        ZIO.attemptBlocking(redis.del("{q:0}:ready")).unit
      for
        conf    <- ZIO.service[QueueConfig]
        _       <- boot(conf)(plant)
        refused <- boot(conf)(KeyLayout.accept(conf)).exit
        _       <- boot(conf)(uproot)
        emptied <- boot(conf)(KeyLayout.accept(conf)).exit
      yield assertTrue(
        refused.causeOption.flatMap(_.failureOption).exists {
          case Misconfigured(reason) => reason.contains("drained")
          case _                     => false
        },
        emptied.isSuccess,
      )
    },
    test("a schema the code does not expect refuses the boot, and accept records the code's own") {
      // The store claims a schema this code never wrote — the shape of an incompatible predecessor. The
      // boot must refuse before touching structures it would misread, and accept (drained: markers only)
      // re-stamps the code's version.
      val predecessor = Connection.use: redis =>
        ZIO.attemptBlocking(redis.set("dkq:layout:schema", "999".getBytes)).unit
      for
        conf     <- ZIO.service[QueueConfig]
        _        <- boot(conf)(predecessor)
        refused  <- boot(conf)(KeyLayout.verify(conf)).exit
        _        <- boot(conf)(KeyLayout.accept(conf))
        restored <- boot(conf)(KeyLayout.verify(conf)).exit
      yield assertTrue(
        refused.causeOption.flatMap(_.failureOption).exists {
          case Misconfigured(reason) => reason.contains("schema") && reason.contains("999")
          case _                     => false
        },
        restored.isSuccess,
      )
    },
  ).provideSomeShared[Scope](substrate) @@ TestAspect.sequential @@ TestAspect.timeout(3.minutes)
