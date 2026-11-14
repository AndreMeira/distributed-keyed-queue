package homelab.keyedqueue.domain.service.maintenance


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.service.persistence.QueueStore
import zio.*


/**
 * The loop that appends scheduled messages to their key's line once they fall due.
 *
 * Woken by the [[DueTimer]]: on every tick it asks the store to move what is due, then asks the timer for a tick
 * at the next due time the store reports. Runs on every instance without coordination, as the watchdog does: a
 * move is idempotent, so one instance moves an entry and the others find it gone, and no instance's death stops
 * scheduled messages from arriving (the others' backstops move them, late).
 *
 * See `docs/research/tick-driven-mover.md`.
 *
 * @param store where scheduled messages wait
 * @param timer what wakes it, and what it asks for the next wake-up
 * @param config how much one call may move
 */
final class ScheduleMover(store: QueueStore, timer: DueTimer, config: ScheduleMover.Config):

  /**
   * Move due messages on every tick, for ever.
   *
   * @return never completes
   */
  def run: UIO[Nothing] = pass.forever

  /**
   * Wait for a tick, then move.
   *
   * @return noop, once the move and the request for the next tick are made
   */
  private def pass: UIO[Unit] = timer.tick *> move

  /**
   * One call to the store, and what follows from its answer.
   *
   * @return noop; a failure is logged and the loop continues, because a store that is briefly unavailable is
   *         not a reason to stop delivering for ever (the next backstop retries)
   */
  private def move: UIO[Unit] =
    store.moveDue(config.limit).foldZIO(failed, moved)

  /**
   * Report a move that failed.
   *
   * @param error what the store reported
   * @return noop
   */
  private def failed(error: ApplicationError): UIO[Unit] =
    ZIO.logWarning(s"moving due scheduled messages failed: ${error.message}")

  /**
   * Report a move, then either move again at once or ask the timer for the next due time.
   *
   * A full call means more may be due right now, so it is repeated without waiting for a tick, as the watchdog
   * repeats a full sweep; going through the timer would cost up to a quantum. `handled > 0` keeps a
   * non-positive limit from turning an empty call into an immediate one, for ever.
   *
   * @param result what the store moved
   * @return noop
   */
  private def moved(result: QueueStore.Moved): UIO[Unit] =
    val handled = result.moved + result.discarded
    movedInfo(result) *> (if result.full && handled > 0 then move else next(result))

  /**
   * Ask the timer for a tick when the next scheduled message falls due.
   *
   * @param result what the store reported, including when the next entry is due
   * @return noop; nothing is asked when nothing is scheduled, and the backstop fires regardless
   */
  private def next(result: QueueStore.Moved): UIO[Unit] =
    result.nextDueIn match
      case Some(in) => timer.triggerIn(in)
      case None     => ZIO.unit

  /**
   * Report what a call moved, when it moved anything, and any part of the store it could not reach.
   *
   * @param result what the store moved
   * @return noop
   */
  private def movedInfo(result: QueueStore.Moved): UIO[Unit] =
    val counts = s"moved=${result.moved} discarded=${result.discarded}"
    if result.failed > 0 then ZIO.logWarning(s"moving due scheduled messages: ${result.failed} part(s) unreachable; $counts")
    else if result.moved == 0 && result.discarded == 0 then ZIO.unit
    else ZIO.logInfo(s"moved due scheduled messages: $counts")


object ScheduleMover:

  /**
   * The slice of configuration the mover needs.
   *
   * @param limit the most entries one call moves per part of the store
   */
  final case class Config(limit: Int)
