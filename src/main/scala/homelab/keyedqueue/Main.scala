package homelab.keyedqueue


import homelab.keyedqueue.application.grpc.GrpcApplication
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
   * The whole service lives in one scope, so a failure anywhere — a connection that will not open, a script
   * the server rejects — closes the connections and the claimers on the way out rather than leaving a
   * half-started process behind.
   *
   * @return never completes successfully; aborts with whatever prevented startup
   */
  override def run: ZIO[Any, Any, Any] =
    ZIO.scoped:
      for
        config <- QueueConfig.load
        _      <- GrpcApplication.serve(config)
      yield ()
