package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.infrastructure.redis.Connection.Commands
import homelab.keyedqueue.infrastructure.redis.script.*
import zio.*


/**
 * The three lock scripts, loaded once and held as the calls they can make — the lock's [[Scripts]].
 *
 * '''Two static keys, shared by every lock.''' Unlike the queue, whose keys are per-key and built from a
 * [[Namespace]], all locks live in one `held` zset and one `fence` hash, with the lock's name as a member
 * or field. So the keys are constant, and all three scripts touch the same two — see
 * [[homelab.keyedqueue.infrastructure.redis.script.LockKeys]] — which keeps them in one cluster slot.
 *
 * @param acquire takes a named lock, reclaiming an expired lease inline
 * @param release frees a lock the caller still holds
 * @param refresh extends a held lock's lease
 */
final case class LockScripts(
  acquire: LockAcquireScript,
  release: LockReleaseScript,
  refresh: LockRefreshScript,
)


object LockScripts:

  /**
   * Register the three lock scripts, so a missing or unparseable one fails at startup.
   *
   * @return the calls they make; aborts with `RedisFailure` when one is missing or rejected
   */
  def make: ZIO[Commands, RedisFailure, LockScripts] =
    for
      acquire <- LockAcquireScript.make
      release <- LockReleaseScript.make
      refresh <- LockRefreshScript.make
    yield LockScripts(acquire, release, refresh)
