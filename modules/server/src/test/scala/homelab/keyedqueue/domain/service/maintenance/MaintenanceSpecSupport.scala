package homelab.keyedqueue.domain.service.maintenance


import zio.*
import zio.test.Live


/**
 * Bounded waits for the specs that drive the schedule timer and mover on the `TestClock`.
 *
 * A wait that joins a fiber or polls a count for ever turns a regression into a hang; these give up after
 * [[patience]] and fail the test, saying what did not happen. A fiber that finished by failing or dying fails the
 * test too, with its cause: finishing is not succeeding. Moving the `TestClock` runs what fell due before it
 * returns, so in a passing test the wait ends at once.
 *
 * The patience is measured on the live clock, not in yields: on a loaded machine a forked fiber may not be scheduled
 * within any fixed number of yields, which made a count of them flaky. The live clock is safe here because every
 * wait starts after the test has moved the `TestClock`, so what it waits for needs no more test time.
 */
object MaintenanceSpecSupport {

  /** How long, on the live clock, a wait lasts before it gives up. */
  val patience: Duration = 5.seconds

  /**
   * Wait until a fiber has succeeded, or fail.
   *
   * @param fiber the fiber to wait on
   * @param what what its succeeding means, for the failure
   * @return noop once it has succeeded; fails with what did not happen when it has not finished within the
   *         patience, and with its cause when it finished by failing or dying
   */
  def done(fiber: Fiber[Any, Any], what: String): IO[String, Unit] =
    Live.live(fiber.await.timeout(patience)).flatMap(settled(what))

  /**
   * What the wait on the fiber found.
   *
   * @param what what its finishing means, for the failure
   * @param exit its exit, when it finished within the patience
   * @return noop when it has succeeded; a failure, with the cause when it failed or died and with what did not
   *         happen when the patience ran out
   */
  private def settled(what: String)(exit: Option[Exit[Any, Any]]): IO[String, Unit] =
    exit match
      case Some(Exit.Success(_))     => ZIO.unit
      case Some(Exit.Failure(cause)) => ZIO.fail(s"$what failed: ${cause.prettyPrint}")
      case None                      => ZIO.fail(s"$what did not happen")
}
