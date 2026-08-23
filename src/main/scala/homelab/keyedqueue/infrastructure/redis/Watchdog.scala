package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.QueueName
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
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
 * @param store any connection — the sweep does not block, so it need not be a claimer's
 * @param config how often, and how much per pass
 * @param queues the queues this instance has served
 */
final class Watchdog(store: QueueStore, config: QueueConfig, queues: Ref[Set[QueueName]]):

  /**
   * Remember a queue, so it is swept from now on.
   *
   * @param queue the queue this instance just served
   * @return noop
   */
  def watch(queue: QueueName): UIO[Unit] = queues.update(_ + queue)

  /**
   * Sweep every known queue, for ever.
   *
   * A sweep that hits its limit is repeated at once rather than after the interval: hitting the limit means
   * there is more to repair, and waiting would leave work stranded for no reason.
   *
   * @return never completes successfully
   */
  def run: URIO[Any, Nothing] =
    (sweepAll *> ZIO.sleep(config.sweepInterval)).forever

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
        error => ZIO.logWarning(s"sweep of $queue failed: ${error.message}"),
        swept =>
          val touched = swept.reclaimed.size + swept.recovered.size + swept.released.size
          ZIO
            .logInfo(
              s"swept $queue: reclaimed=${swept.reclaimed.size} recovered=${swept.recovered.size} " +
                s"released=${swept.released.size}"
            )
            .unless(swept.isEmpty)
            .unit
            // A full pass means there is more waiting; do not make it wait for the next tick.
            *> sweep(queue).when(touched >= config.sweepLimit).unit,
      )


object Watchdog:

  /**
   * Start the repair loop for the life of the scope.
   *
   * @param store the connection it sweeps with
   * @param config the interval and the per-pass limit
   * @return the watchdog, so callers can tell it which queues exist
   */
  def make(store: QueueStore, config: QueueConfig): ZIO[Scope, Nothing, Watchdog] =
    for
      queues   <- Ref.make(Set.empty[QueueName])
      watchdog  = Watchdog(store, config, queues)
      _        <- watchdog.run.forkScoped
    yield watchdog
