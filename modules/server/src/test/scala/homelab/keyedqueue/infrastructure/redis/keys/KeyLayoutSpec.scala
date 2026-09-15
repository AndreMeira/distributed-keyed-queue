package homelab.keyedqueue.infrastructure.redis.keys


import homelab.keyedqueue.SpecHelper
import homelab.keyedqueue.infrastructure.configuration.{Misconfigured, QueueConfig}
import homelab.keyedqueue.infrastructure.redis.{Connection, RedisSpecSupport}
import zio.*
import zio.test.*


/**
 * The boot-time schema check, against real Valkey.
 *
 * The three paths that matter: a first boot records and proceeds, a matching boot proceeds, and a
 * mismatched boot refuses with both versions in the message — before anything could be served against
 * structures the code would misread. Deleting the marker is the whole remedy an operator has, and the
 * second test drives it.
 */
object KeyLayoutSpec extends ZIOSpecDefault:

  /**
   * Run a layout effect the way boot does: on the suite's connection.
   *
   * @param effect what to run against the store
   * @return what the effect returns
   */
  private def boot[A](effect: ZIO[Connection.Commands, Any, A]): ZIO[Connection, Any, A] =
    ZIO.serviceWithZIO[Connection](_.provide(effect))

  def spec: Spec[TestEnvironment & Scope, Any] = {
    suite("KeyLayout")(
      test("a first boot records the layout, and a matching boot passes ever after") {
        for
          conf  <- ZIO.service[QueueConfig]
          _     <- boot(KeyLayout.of(conf.cluster).verify)
          again <- boot(KeyLayout.of(conf.cluster).verify).exit
        yield assertTrue(again.isSuccess)
      },
      test("a layout the code does not expect refuses the boot, and deleting the marker clears it") {
        // The store claims a layout this code never wrote — the shape of an incompatible predecessor. The
        // boot must refuse before touching structures it would misread. Deleting the marker is the whole
        // remedy an operator has: the next boot records its own.
        val predecessor = Connection.use: redis =>
          ZIO.attemptBlocking(redis.set("dkq:layout:schema", "999".getBytes)).unit
        val drained     = Connection.use: redis =>
          ZIO.attemptBlocking(redis.del("dkq:layout:schema")).unit
        for
          conf     <- ZIO.service[QueueConfig]
          _        <- boot(predecessor)
          refused  <- boot(KeyLayout.of(conf.cluster).verify).exit
          _        <- boot(drained)
          restored <- boot(KeyLayout.of(conf.cluster).verify).exit
        yield assertTrue(
          refused.causeOption.flatMap(_.failureOption).exists {
            case Misconfigured(reason) => reason.contains("schema") && reason.contains("999")
            case _                     => false
          },
          restored.isSuccess,
        )
      },
    ) @@ SpecHelper.Aspect.common
  }.provideSomeShared[Scope](RedisSpecSupport.config(30.seconds) >+> RedisSpecSupport.layer)
