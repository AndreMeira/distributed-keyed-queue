package homelab.keyedqueue


import homelab.common.error.ApplicationError
import homelab.keyedqueue.application.grpc.v1.GrpcApplication
import homelab.keyedqueue.infrastructure.configuration.{ Misconfigured, QueueConfig }
import homelab.keyedqueue.infrastructure.redis.{ Connection, Layout }
import homelab.keyedqueue.infrastructure.redis.Module as RedisModule
import zio.*


/**
 * Entry point: dispatch on CLI args to pick a run mode.
 *
 * No arguments serves; `layout accept` records this configuration's layout in the store and exits — the
 * deliberate half of the ceremony the boot check in [[Layout]] enforces. An operational task is a mode of
 * the same binary rather than a second one, so it runs with exactly the configuration the service would.
 */
object Main extends ZIOAppDefault:

  /**
   * Read the configuration and run the mode the arguments name.
   *
   * Everything lives in one layer graph, so a failure anywhere — a connection that will not open, a script
   * the server rejects — tears down whatever was already built rather than leaving a half-started process.
   *
   * @return completes when a one-shot mode does; serving never completes successfully; aborts with whatever
   *         prevented startup, or with `Misconfigured` when the arguments name no mode
   */
  override def run: ZIO[ZIOAppArgs, ApplicationError, Unit] =
    for
      args <- getArgs
      conf <- QueueConfig.load
      _    <- args.toList match
                case Nil                         => GrpcApplication.serve(conf)
                case "layout" :: "accept" :: Nil => accept(conf)
                case other                       =>
                  ZIO.fail(Misconfigured(s"unknown run mode '${other.mkString(" ")}': run with no arguments to serve, or 'layout accept'"))
    yield ()

  /**
   * Record this configuration's layout in the store, and exit.
   *
   * @param conf where the store and the bucket count come from
   * @return noop; aborts when the store cannot be reached
   */
  private def accept(conf: QueueConfig): ZIO[Any, ApplicationError, Unit] =
    ZIO
      .serviceWithZIO[Connection](_.provide(Layout.accept(conf)))
      .provide(ZLayer.succeed(conf), RedisModule.connection)
