package homelab.keyedqueue.domain.service.maintenance


import homelab.keyedqueue.domain.types.QueueName
import zio.UIO


/**
 * Whatever keeps a queue's abandoned work moving again, told which queues exist.
 *
 * A queue nobody has touched has nothing to repair, and a store cannot enumerate the queues it might one day
 * hold, so the apply cases that serve a queue announce it here. What happens then is the implementation's
 * business: this substrate sweeps on a timer, because Redis leases are entries in a sorted set that somebody
 * has to look at.
 *
 * '''It may turn out to be an adapter detail.''' A backend that expires claims itself — a store with native
 * lease semantics, or one where a scheduled job in the database does the sweeping — would implement this as
 * a no-op. If that happens, the honest conclusion is that repair was never a domain concern and this port
 * should go, rather than that every backend must pretend to have a watchdog.
 */
trait Watchdog:

  /**
   * Note that this queue is being served, so it is repaired from now on.
   *
   * Idempotent, and cheap enough to call on every request rather than tracked by the caller.
   *
   * @param queue the queue just served
   * @return noop
   */
  def watch(queue: QueueName): UIO[Unit]
