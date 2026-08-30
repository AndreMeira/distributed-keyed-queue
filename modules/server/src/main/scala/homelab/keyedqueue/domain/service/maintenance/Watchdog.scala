package homelab.keyedqueue.domain.service.maintenance


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.QueueName
import zio.*


/**
 * The repair loop: revoke lapsed claims, recover keys from workers that died mid-claim, release keys whose
 * retry backoff has elapsed.
 *
 * Runs on every instance without coordination, because the sweep is idempotent — reclaiming an
 * already-reclaimed key does nothing. No leader election, and no single instance whose death stops repair.
 *
 * '''It only sweeps queues it has seen.''' A queue nobody has touched has nothing to repair; one that this
 * instance has served is remembered for the life of the process, so its keys keep being swept even after the
 * traffic stops.
 *
 * '''Not a port, and not an adapter.''' Repairing abandoned work is a rule about the queue, not about any
 * store: everything below is `sweep` on a timer, and `sweep` is already something every backend implements.
 * A store with native lease expiry says so by returning nothing swept, and this keeps ticking harmlessly —
 * which is a cheaper answer than every backend having to supply a watchdog of its own to satisfy a port.
 *
 * @param store where the work to repair lives
 * @param config how often, and how much per pass
 * @param queues the queues this instance has served
 */
final class Watchdog(store: QueueStore, config: Watchdog.Config, queues: Ref[Set[QueueName]]):

  /**
   * Note that this queue is being served, so it is repaired from now on.
   *
   * A store cannot enumerate the queues it might one day hold, so the use cases that serve a queue announce
   * it here. Idempotent, and cheap enough to call on every request rather than tracked by the caller.
   *
   * Remembered '''in memory''', which is the one real limitation. A restarted instance therefore sweeps only
   * the queues it has served since booting. That is tolerable rather than accidental: its peers keep sweeping
   * the rest, a queue with no traffic anywhere has nothing to repair, and the alternative — a persisted
   * registry of every queue ever seen — would need pruning logic of its own to stop growing for ever.
   *
   * @param queue the queue just served
   * @return noop
   */
  def watch(queue: QueueName): UIO[Unit] = queues.update(_ + queue)

  /**
   * Sweep every known queue, for ever.
   *
   * A sweep that hits its limit is repeated at once rather than after the interval: hitting the limit means
   * there is more to repair, and waiting would leave work stranded for no reason.
   *
   * '''`spaced`, not `fixed`.''' The interval is a gap *between* passes, not a rate to keep up with. A pass
   * that overran the interval means the store is busy — the moment when firing the next one immediately, as
   * a fixed rate would, is exactly the wrong thing to do.
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
          val touched = swept.reclaimed.size + swept.recovered.size + swept.released.size
          // A full pass means there is more waiting; do not make it wait for the next tick.
          sweptInfo(queue, swept) *> sweep(queue).when(touched >= config.sweepLimit).unit,
      )

  /**
   * Report a sweep that failed.
   *
   * A warning rather than a failure, and it stops here: a store that is briefly unavailable is not a reason
   * to stop repairing for ever, and the next pass is one interval away. What it costs is a delay in recovery,
   * which the lease already tolerates.
   *
   * @param queue the queue whose sweep failed
   * @param error what the store reported
   * @return noop
   */
  private def sweptWarn(queue: QueueName, error: QueueError): UIO[Unit] =
    ZIO.logWarning(s"sweep of $queue failed: ${error.message}")

  /**
   * Report what a pass repaired, when it repaired anything.
   *
   * '''Silent on an empty pass, deliberately.''' Every instance sweeps every queue it has served, every few
   * seconds; logging "found nothing" would be almost every line, and would bury the ones that say a claim
   * lapsed or a worker died — which are the only lines anybody reads this log for.
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
          s"recovered=${swept.recovered.size}" +
          s" released=${swept.released.size}"
      )


object Watchdog:

  /**
   * The slice of configuration the repair loop needs.
   *
   * Its own type so the domain never sees a file full of infrastructure settings — the narrowing happens in
   * the configuration module, as it does for the use cases.
   *
   * @param sweepInterval the gap between passes over the known queues
   * @param sweepLimit the most entries one pass handles per queue
   */
  final case class Config(sweepInterval: Duration, sweepLimit: Int)

  /**
   * Start the repair loop for the life of the scope.
   *
   * @param store where the work to repair lives
   * @param config the interval and the per-pass limit
   * @return the watchdog, so callers can tell it which queues exist
   */
  def make(store: QueueStore, config: Config): ZIO[Scope, Nothing, Watchdog] =
    for
      queues  <- Ref.make(Set.empty[QueueName])
      watchdog = Watchdog(store, config, queues)
      _       <- watchdog.run.forkScoped
    yield watchdog
