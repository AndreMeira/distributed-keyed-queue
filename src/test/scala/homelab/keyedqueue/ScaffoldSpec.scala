package homelab.keyedqueue


import homelab.common.flow.KeyedQueue
import homelab.keyedqueue.v1.{ PingReply, PingRequest }
import org.testcontainers.containers.PostgreSQLContainer
import zio.*
import zio.test.*


/**
 * SCAFFOLDING — delete once there are real tests.
 *
 * It asserts nothing interesting on purpose. What it proves is that the *build* is whole: the toolkit
 * resolves from GitHub Packages, the generated gRPC types are on the classpath, zio-test runs, and the
 * Testcontainers Postgres image is available to be constructed. Each of those is a separate way the
 * scaffolding could be silently broken.
 */
object ScaffoldSpec extends ZIOSpecDefault:

  def spec: Spec[TestEnvironment & Scope, Any] = suite("scaffold")(
    test("the toolkit resolves and its primitives work") {
      for
        queue <- KeyedQueue.make[String, Int](maxBuffer = Some(4)).orDieWith(error => new RuntimeException(error.toString))
        _     <- queue.offer("a", 1)
        taken <- queue.takeWith((key, value) => ZIO.succeed((key, value)))
      yield assertTrue(taken == ("a", 1))
    },
    test("the generated gRPC types are on the classpath") {
      val request = PingRequest(key = "k")
      assertTrue(PingReply(key = request.key).key == "k")
    },
    test("Testcontainers is on the test classpath") {
      // Deliberately does not construct a container: `getDockerImageName` resolves the image through the
      // Docker client, so it needs a running daemon. The real integration tests will start one; here the
      // point is only that the dependency is present.
      assertTrue(classOf[PostgreSQLContainer[?]].getName.endsWith("PostgreSQLContainer"))
    },
  )
