package homelab.keyedqueue.domain.service.maintenance


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.service.lock.LockStore
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
   * A pass that hits its limit is repeated at once: hitting the limit means there is more to remove. The
   * interval is a gap between passes, not a rate.
   *
   * @return never completes on its own; the schedule has no end
   */
  def run: UIO[Unit] =
    pass.repeat(Schedule.spaced(config.interval).unit)

  /**
   * One trim, repeated while full.
   *
   * @return noop; a failure is logged and the loop continues, because a store that is briefly unavailable
   *         is not a reason to stop cleaning for ever
   */
  private def pass: UIO[Unit] =
    store
      .trim(config.grace, config.limit)
      .foldZIO(
        error => trimWarn(error),
        // A full pass means there is more waiting; do not make it wait for the next tick. `nonEmpty`
        // keeps a non-positive limit from turning an empty pass into an immediate one, for ever.
        freed => trimInfo(freed) *> pass.when(freed.nonEmpty && freed.size >= config.limit).unit,
      )

  /**
   * Report a trim that failed.
   *
   * A warning, not a failure: the loop continues, and the next pass is one interval away.
   *
   * @param error what the store reported
   * @return noop
   */
  private def trimWarn(error: ApplicationError): UIO[Unit] =
    ZIO.logWarning(s"lock trim failed: ${error.message}")

  /**
   * Report what a pass removed, when it removed anything.
   *
   * Named, not counted: an abandoned hold is a holder that died without releasing, which is an anomaly
   * worth pointing at.
   *
   * @param freed the locks freed
   * @return noop
   */
  private def trimInfo(freed: Chunk[LockName]): UIO[Unit] =
    ZIO.logInfo(s"trimmed abandoned locks: ${freed.mkString(", ")}").unless(freed.isEmpty).unit


object LockCleanup:

  /**
   * The slice of configuration the hygiene loop needs.
   *
   * @param interval the gap between passes
   * @param grace how long past lease expiry a hold survives before it may be removed
   * @param limit the most holds one pass removes
   */
  final case class Config(interval: Duration, grace: Duration, limit: Int)

  /**
   * Start the hygiene loop for the life of the scope.
   *
   * @param store where the holds live
   * @param config the interval, the grace, and the per-pass limit
   * @return the loop, already running
   */
  def make(store: LockStore, config: Config): ZIO[Scope, Nothing, LockCleanup] =
    for
      cleanup <- ZIO.succeed(LockCleanup(store, config))
      _       <- cleanup.run.forkScoped
    yield cleanup
