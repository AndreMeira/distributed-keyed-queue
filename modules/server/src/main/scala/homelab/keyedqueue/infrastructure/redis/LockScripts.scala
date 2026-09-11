package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.infrastructure.redis.Connection.Commands
import homelab.keyedqueue.infrastructure.redis.script.*
import zio.*


/**
 * The lock scripts, loaded once and held as the calls they can make — the lock's [[Scripts]].
 *
 * '''Static keys, shared by every lock.''' Unlike the queue, whose keys are per-key and built from a
 * [[Namespace]], all locks live in shared structures with the lock's name as a member or field — see
 * [[homelab.keyedqueue.infrastructure.redis.script.LockKeys]] — which keeps them in one cluster slot.
 *
 * @param acquire enters for a named lock — an immediate grant or a tail ticket
 * @param grant asks for a queued waiter's turn
 * @param abandon withdraws a ticket
 * @param tryAcquire takes a lock only if free now with nobody queued
 * @param release frees a lock the caller still holds
 * @param refresh extends a held lock's lease
 * @param trim removes holds and waiter lists abandoned past a grace window
 */
final case class LockScripts(
  acquire: LockAcquireScript,
  grant: LockGrantScript,
  abandon: LockAbandonScript,
  tryAcquire: LockTryScript,
  release: LockReleaseScript,
  refresh: LockRefreshScript,
  trim: LockTrimScript,
)


object LockScripts:

  /**
   * Register the lock scripts, so a missing or unparseable one fails at startup.
   *
   * @return the calls they make; aborts with `RedisFailure` when one is missing or rejected
   */
  def make: ZIO[Commands, RedisFailure, LockScripts] =
    for
      acquire    <- LockAcquireScript.make
      grant      <- LockGrantScript.make
      abandon    <- LockAbandonScript.make
      tryAcquire <- LockTryScript.make
      release    <- LockReleaseScript.make
      refresh    <- LockRefreshScript.make
      trim       <- LockTrimScript.make
    yield LockScripts(acquire, grant, abandon, tryAcquire, release, refresh, trim)
