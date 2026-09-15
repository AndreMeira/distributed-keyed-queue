package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.SpecHelper
import org.testcontainers.containers.GenericContainer
import zio.*


object Container {
  // @todo ValkeyContainer?
  type Type = GenericContainer[?]

  /**   */
  val port = 6379

  /**   */
  private val version = "valkey/valkey:8.1-alpine"

  /**
   *
   * @return
   */
  def run: ZIO[Scope, ApplicationError, GenericContainer[?]] =
    ZIO.acquireRelease(start)(stop).mapError { err =>
      SpecHelper.Failure("Failed to start Valkey container: " + err.getMessage)
    }

  /**
   *
   * @return
   */
  private def pinAPIVersion: Task[String] = ZIO.attempt:
    // Docker Engine 29 rejects the API version docker-java negotiates by default with an HTTP 400;
    // pinning it is what the toolkit's Testcontainers specs do too.
    java.lang.System.setProperty("api.version", "1.40")

  /**
   * Start a Valkey for the suite.
   *
   * @return the started container; aborts with whatever the Docker client refused with
   */
  private def start: Task[GenericContainer[?]] =
    pinAPIVersion *> ZIO.attemptBlocking:
      // GenericContainer is self-referentially generic (SELF extends GenericContainer), which Scala infers
      // as Nothing — hence the explicit wildcard and no chaining.
      val container: GenericContainer[?] = GenericContainer(version)
      container.setExposedPorts(java.util.List.of(Integer.valueOf(port)))
      container.start()
      container

  /**
   * Stop a container, whatever became of the suite that used it.
   *
   * @param container the container to stop
   * @return noop
   */
  private def stop(container: GenericContainer[?]): UIO[Unit] =
    ZIO.attemptBlocking(container.stop()).ignore
}
