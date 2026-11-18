package homelab.keyedqueue.domain.service.maintenance


import homelab.keyedqueue.domain.service.persistence.LockStore
import homelab.keyedqueue.domain.types.LockName
import zio.*


/**
 * The lock hygiene loop: remove holds abandoned past the grace window — the lock's [[Watchdog]].
 *
 * Runs on every instance without coordination: a trim is idempotent, so there is no leader election and no
 * single instance whose death stops cleanup. Unlike the watchdog it keeps no registry — every lock lives in
 * one shared structure, so a pass sees them all.
 *
 * @param store where the holds live
 * @param config how often, how much grace, and how much per pass
 */
final class LockCleanup(store: LockStore, config: LockCleanup.Config):

  /**
   * Trim abandoned holds, for ever.
   *
   * A pass that hits its limit is repeated at once: hitting the limit means there may be more to remove.
   * The store applies the limit per atomic step (per partition, on Redis), and this judges the total of
   * holds freed, so a pass whose steps were each partly full can be repeated with less to do; one with a
   * step that freed a full `limit` of holds is never left waiting for the tick. Only holds are counted: a pass
   * that filled its budget of dead waiter lists but freed fewer holds is not repeated, and the remaining
   * lists wait for the next interval. The interval is a gap between passes, not a rate.
   *
   * @return never completes on its own; the schedule has no end
   */
  def run: UIO[Unit] =
    pass.repeat(Schedule.spaced(config.interval).unit)

  /**
   * One trim, repeated while full.
   *
   * @return noop; a failure is recorded by the store's trace and the loop continues, because a store that is
   *         briefly unavailable is not a reason to stop cleaning for ever
   */
  private def pass: UIO[Unit] =
    store.trim(config.grace, config.limit).flatMap(trimmed).ignore

  /**
   * Trim again at once when the pass was full.
   *
   * A full pass means there is more waiting; it does not wait for the next tick. `nonEmpty` keeps a
   * non-positive limit from turning an empty pass into an immediate one, for ever.
   *
   * @param freed the locks the pass freed
   * @return noop, once nothing more is due
   */
  private def trimmed(freed: Chunk[LockName]): UIO[Unit] =
    pass.when(freed.nonEmpty && freed.size >= config.limit).unit


object LockCleanup:

  /**
   * The slice of configuration the hygiene loop needs.
   *
   * @param interval the gap between passes
   * @param grace how long past lease expiry a hold survives before it may be removed
   * @param limit the most holds one pass removes
   */
  final case class Config(interval: Duration, grace: Duration, limit: Int)
