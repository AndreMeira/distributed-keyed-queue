package homelab.keyedqueue.e2e


import zio.*

import java.io.File
import scala.sys.process.{ Process, ProcessLogger }


/**
 * The deployment under test, as `docker compose` sees it.
 *
 * Killing and restarting an instance is a *test operation* here, not a fixture detail: "an instance dies
 * holding claims" is one of the properties the suite exists to prove, and the only honest way to stage it is
 * to kill the process for real.
 *
 * Its own project name keeps this stack disjoint from the dev one in `docker-compose.yml`, so a suite run
 * never tears down something a developer was using.
 */
object Compose:

  /** The compose project name; also the prefix on every container this suite creates. */
  val project: String = "dkq-e2e"

  /** The instance names, which are both compose service names and the suite's names for them. */
  val instances: Chunk[String] = Chunk("dkq-a", "dkq-b")

  private val file = "docker-compose.e2e.yml"

  /**
   * Bring the stack up and wait for every service to report healthy.
   *
   * @return noop; fails when compose does, which usually means the image has not been built
   */
  def up: Task[Unit] = run("up", "-d", "--wait").unit

  /**
   * Tear it down, volumes included.
   *
   * Failures are swallowed: this runs as a finalizer, where a teardown error would replace whatever the test
   * was actually failing on.
   *
   * @return noop
   */
  def down: UIO[Unit] = run("down", "-v", "--remove-orphans").ignore

  /**
   * SIGKILL one instance.
   *
   * Kill rather than stop, and no grace period: a stopped service gets to close its Redis connections and
   * hand back what it was holding, which is the *opposite* of the failure being staged.
   *
   * @param instance the service to kill
   * @return noop; fails when compose does
   */
  def kill(instance: String): Task[Unit] = run("kill", instance).unit

  /**
   * Bring one instance back, and wait until it is healthy again.
   *
   * @param instance the service to restart
   * @return noop; fails when compose does
   */
  def revive(instance: String): Task[Unit] = run("up", "-d", "--wait", instance).unit

  /**
   * What an instance logged, for when an assertion fails and the reason is server-side.
   *
   * @param instance the service to read
   * @return its logs; fails when compose does
   */
  def logs(instance: String): Task[String] = run("logs", "--no-color", instance)

  /**
   * Run one compose command against this suite's project.
   *
   * Output is captured rather than inherited so a failure can carry compose's own explanation — which is
   * nearly always the actionable part ("image not found", "port already allocated").
   *
   * @param arguments the compose subcommand and its arguments
   * @return everything the command printed; fails with the exit code and output when it did not succeed
   */
  private def run(arguments: String*): Task[String] =
    root.flatMap: directory =>
      ZIO.attemptBlocking:
        val output  = StringBuilder()
        val collect = ProcessLogger: line =>
          val _ = output.append(line).append('\n')
        val command = Seq("docker", "compose", "-p", project, "-f", file) ++ arguments
        val status  = Process(command, directory).!(collect)
        if status == 0 then output.toString
        else throw IllegalStateException(s"`${command.mkString(" ")}` exited $status:\n$output")

  /**
   * Find the directory holding the compose file, by walking up from wherever the JVM started.
   *
   * Not a constant, because the working directory differs between `sbt e2e/test` (the subproject's own
   * directory) and an IDE run (usually the repository root), and a harness that only works one of those ways
   * is a harness people stop using.
   *
   * @return the directory; fails when the compose file is nowhere above the working directory
   */
  private def root: Task[File] =
    ZIO
      .attempt:
        Iterator
          .iterate(File(".").getAbsoluteFile)(_.getParentFile)
          .takeWhile(_ != null)
          .find(directory => File(directory, file).isFile)
      .someOrFail(IllegalStateException(s"$file not found above ${File(".").getAbsolutePath}"))
