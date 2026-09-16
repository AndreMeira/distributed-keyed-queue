package homelab.keyedqueue.domain.service.maintenance


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.QueueName
import zio.*


/**
 * The repair loop: revoke lapsed claims, and release keys whose retry backoff has elapsed.
 *
 * Runs on every instance without coordination: the sweep is idempotent, so there is no leader election and
 * no single instance whose death stops repair.
 *
 * It sweeps only the queues it has seen: a queue this instance has served is remembered for the life of
 * the process, so its keys keep being swept after the traffic stops.
 *
 * @param store where the work to repair lives
 * @param config how often, and how much per pass
 * @param queues the queues this instance has served
 */
final class Watchdog(store: QueueStore, config: Watchdog.Config, queues: Ref[Set[QueueName]]):

  /**
   * Note that this queue is being served, so it is repaired from now on.
   *
   * Idempotent, and cheap enough to call on every request. Remembered in memory, so a restarted instance
   * sweeps only the queues it has served since booting.
   *
   * @param queue the queue just served
   * @return noop
   */
  def watch(queue: QueueName): UIO[Unit] = queues.update(_ + queue)

  /**
   * Sweep every known queue, for ever.
   *
   * A sweep that hits its limit is repeated at once: hitting the limit means there is more to repair. The
   * interval is a gap between passes, not a rate.
   *
   * @return never completes on its own; the schedule has no end
   */
  def run: UIO[Unit] =
    sweepAll.repeat(Schedule.spaced(config.sweepInterval).unit)

  /**
   * One pass over the known queues.
   *
   * @return noop; a failure is logged and the loop continues, because a store that is briefly unavailable is
   *         not a reason to stop repairing for ever
   */
  private def sweepAll: UIO[Unit] =
    queues.get.flatMap(ZIO.foreachDiscard(_)(sweep))

  /**
   * Sweep one queue until it is quiet.
   *
   * @param queue the queue to repair
   * @return noop
   */
  private def sweep(queue: QueueName): UIO[Unit] =
    store
      .sweep(queue, config.sweepLimit)
      .foldZIO(
        error => sweptWarn(queue, error),
        swept =>
          val touched = swept.reclaimed.size + swept.released.size
          // A full pass means there is more waiting; do not make it wait for the next tick. `touched > 0`
          // keeps a non-positive limit from turning an empty pass into an immediate one, for ever.
          sweptInfo(queue, swept) *> sweep(queue).when(touched > 0 && touched >= config.sweepLimit).unit,
      )

  /**
   * Report a sweep that failed.
   *
   * A warning, not a failure: the loop continues, and the next pass is one interval away.
   *
   * @param queue the queue whose sweep failed
   * @param error what the store reported
   * @return noop
   */
  private def sweptWarn(queue: QueueName, error: ApplicationError): UIO[Unit] =
    ZIO.logWarning(s"sweep of $queue failed: ${error.message}")

  /**
   * Report what a pass repaired, when it repaired anything.
   *
   * Silent on an empty pass, so the only lines are the ones that say a claim lapsed or a worker died.
   *
   * @param queue the queue just swept
   * @param swept what the pass repaired
   * @return noop
   */
  private def sweptInfo(queue: QueueName, swept: QueueStore.Swept): UIO[Unit] =
    if swept.isEmpty then ZIO.unit
    else
      ZIO.logInfo(
        s"swept $queue: " +
          s"reclaimed=${swept.reclaimed.size} " +
          s"released=${swept.released.size}"
      )


object Watchdog:

  /**
   * The slice of configuration the repair loop needs.
   *
   * @param sweepInterval the gap between passes over the known queues
   * @param sweepLimit the most entries one pass handles per queue
   */
  final case class Config(sweepInterval: Duration, sweepLimit: Int)

  /**
   * A watchdog over this store, not yet repairing.
   *
   * Building one allocates what it remembers and nothing else; starting the loop is [[Watchdog.run]], which
   * whoever owns the lifetime forks — see the maintenance module's `init`.
   *
   * @param store where the work to repair lives
   * @param config the interval and the per-pass limit
   * @return the watchdog, so callers can tell it which queues exist
   */
  def make(store: QueueStore, config: Config): UIO[Watchdog] =
    Ref.make(Set.empty[QueueName]).map(Watchdog(store, config, _))
