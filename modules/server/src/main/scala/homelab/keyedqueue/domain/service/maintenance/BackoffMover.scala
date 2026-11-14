package homelab.keyedqueue.domain.service.maintenance


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.service.persistence.QueueStore
import zio.*


/**
 * The loop that makes a held-back key claimable again once its backoff has ended.
 *
 * A nack's wait holds back its key (C6). Woken by its own [[DueTimer]]: on every tick it asks the store to release
 * what has ended, then asks the timer for a tick when the next backoff ends. Runs on every instance without
 * coordination, as the [[ScheduleMover]] does, and with the same shape: a release is idempotent, a full call is
 * repeated at once, and a failure is logged and retried by the backstop.
 *
 * Every call to the store is measured as `BackoffMover.release`; what a pass released is in its log line.
 *
 * See `docs/research/tick-driven-mover.md`.
 *
 * @param monitor what each call to the store is counted and timed against
 * @param store where held-back keys wait
 * @param timer what wakes it, and what it asks for the next wake-up
 * @param config how much one call may release
 */
final class BackoffMover(monitor: Monitor, store: QueueStore, timer: DueTimer, config: BackoffMover.Config):

  /**
   * Release ended backoffs on every tick, for ever.
   *
   * @return never completes
   */
  def run: UIO[Nothing] = pass.forever

  /**
   * Wait for a tick, then release.
   *
   * @return noop, once the release and the request for the next tick are made
   */
  private def pass: UIO[Unit] = timer.tick *> release

  /**
   * One call to the store, and what follows from its answer.
   *
   * @return noop; a failure is logged and the loop continues (the next backstop retries)
   */
  private def release: UIO[Unit] =
    monitor.measure("BackoffMover.release")(store.releaseDue(config.limit)).foldZIO(failed, released)

  /**
   * Report a release that failed.
   *
   * @param error what the store reported
   * @return noop
   */
  private def failed(error: ApplicationError): UIO[Unit] =
    ZIO.logWarning(s"releasing ended backoffs failed: ${error.message}")

  /**
   * Report a release, then either release again at once or ask the timer for the next end.
   *
   * A full call means more may have ended right now, so it is repeated without waiting for a tick. `released > 0`
   * keeps a non-positive limit from turning an empty call into an immediate one, for ever.
   *
   * @param result what the store released
   * @return noop
   */
  private def released(result: QueueStore.Released): UIO[Unit] =
    releasedInfo(result) *> (if result.full && result.released > 0 then release else next(result))

  /**
   * Ask the timer for a tick when the next backoff ends.
   *
   * @param result what the store reported, including when the next backoff ends
   * @return noop; nothing is asked when no backoff is pending, and the backstop fires regardless
   */
  private def next(result: QueueStore.Released): UIO[Unit] =
    result.nextDueIn match
      case Some(in) => timer.triggerIn(in)
      case None     => ZIO.unit

  /**
   * Report what a call released, when it released anything, and any part of the store it could not reach.
   *
   * @param result what the store released
   * @return noop
   */
  private def releasedInfo(result: QueueStore.Released): UIO[Unit] =
    if result.failed > 0 then ZIO.logWarning(s"releasing ended backoffs: ${result.failed} part(s) unreachable; released=${result.released}")
    else if result.released == 0 then ZIO.unit
    else ZIO.logInfo(s"released ended backoffs: released=${result.released}")


object BackoffMover:

  /**
   * The slice of configuration the backoff mover needs.
   *
   * @param limit the most backoffs one call releases per part of the store
   */
  final case class Config(limit: Int)
