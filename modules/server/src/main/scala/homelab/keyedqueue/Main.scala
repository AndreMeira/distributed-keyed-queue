package homelab.keyedqueue


import homelab.common.error.ApplicationError
import homelab.keyedqueue.application.grpc.v1.GrpcApplication
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import zio.*


/**
 * Entry point: read the configuration and serve.
 */
object Main extends ZIOAppDefault:

  /**
   * Read the configuration and serve.
   *
   * Everything lives in one layer graph, so a failure anywhere — a connection that will not open, a script
   * the server rejects — tears down whatever was already built rather than leaving a half-started process.
   *
   * @return never completes successfully; aborts with whatever prevented startup
   */
  override def run: ZIO[ZIOAppArgs, ApplicationError, Unit] =
    for
      conf <- QueueConfig.load
      _    <- GrpcApplication.serve(conf)
    yield ()
