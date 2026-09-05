package homelab.keyedqueue


import homelab.common.error.ApplicationError
import homelab.keyedqueue.application.grpc.v1.GrpcApplication
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import zio.*


/**
 * Entry point.
 *
 * One run mode for now — serve. The shape to grow into is the toolkit's: pattern match on CLI args so an
 * operational task (a purge, a one-off sweep) is a mode of the same binary rather than a second one.
 */
object Main extends ZIOAppDefault:

  /**
   * Read the configuration and serve until interrupted.
   *
   * Everything lives in one layer graph, so a failure anywhere — a connection that will not open, a script
   * the server rejects — tears down whatever was already built rather than leaving a half-started process.
   *
   * @return never completes successfully; aborts with whatever prevented startup
   */
  override def run: ZIO[Any, ApplicationError, Nothing] =
    for
      conf  <- QueueConfig.load
      never <- GrpcApplication.serve(conf)
    yield never
