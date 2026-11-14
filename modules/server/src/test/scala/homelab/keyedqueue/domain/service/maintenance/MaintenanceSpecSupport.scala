package homelab.keyedqueue.domain.service.maintenance

import zio.*


/**
 * Bounded waits for the specs that drive the schedule timer and mover on the `TestClock`.
 *
 * A wait that joins a fiber or polls a count for ever turns a regression into a hang; these give up after a
 * fixed number of yields and fail the test, saying what did not happen. Moving the `TestClock` runs what fell
 * due before it returns, so in a passing test the wait ends within a few yields.
 */
object MaintenanceSpecSupport {

  /** How many times a wait yields before it gives up. */
  val patience: Int = 10_000

  /**
   * Wait until a fiber has finished, or fail.
   *
   * @param fiber the fiber to wait on
   * @param what what its finishing means, for the failure
   * @return noop once it has finished; fails with what did not happen when it has not within the patience
   */
  def done(fiber: Fiber[Any, Any], what: String): IO[String, Unit] =
    doneWithin(fiber, what, patience)

  /**
   * Look at the fiber, and look again after a yield while it runs and patience is left.
   *
   * @param fiber the fiber to wait on
   * @param what what its finishing means, for the failure
   * @param left how many more yields the wait may take
   * @return noop once it has finished; fails when the patience runs out first
   */
  private def doneWithin(fiber: Fiber[Any, Any], what: String, left: Int): IO[String, Unit] =
    fiber.poll.flatMap(settled(fiber, what, left))

  /**
   * What a look at the fiber found, and what follows.
   *
   * @param fiber the fiber to wait on
   * @param what what its finishing means, for the failure
   * @param left how many more yields the wait may take
   * @param exit its exit, when it has finished
   * @return noop when it has finished; another look after a yield; or a failure when the patience is gone
   */
  private def settled(fiber: Fiber[Any, Any], what: String, left: Int)(exit: Option[Exit[Any, Any]]): IO[String, Unit] =
    exit match
      case Some(_)          => ZIO.unit
      case None if left > 0 => ZIO.yieldNow *> doneWithin(fiber, what, left - 1)
      case None             => ZIO.fail(s"$what did not happen")
}
