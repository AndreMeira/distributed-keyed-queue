package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.QueueName
import zio.*


/**
 * Where a wake is delivered: the sink [[WakeListener]] routes entries to.
 *
 * The listener is a transport and is indifferent to what happens next — how many waiters a wake reaches is
 * the sink's business. What a *router* is not free to ignore is whether a wake survives having no audience,
 * because the two implementations answer differently and a queue and a lock depend on opposite answers:
 *
 *   - [[Readiness]] '''keeps''' it. A token offered while the only consumer is mid-claim waits for that
 *     consumer's next look. The queue needs this: its wake says work exists, and work does not stop
 *     existing because nobody happened to be parked.
 *   - [[Broadcast]] '''drops''' it. A wake reaches the mailboxes subscribed at that moment and no others.
 *     The lock can afford it because a waiter subscribes before it enters, so there is no gap for a
 *     release to fall into — and it needs the fan-out, since only the store knows whose turn it is.
 *
 * So a route is not a free choice: sending a queue's stream to a [[Broadcast]] would lose wakes whenever no
 * consumer was parked, and sending a lock's to a [[Readiness]] would hand the one token to a waiter that
 * may not be next, leaving the head asleep. Both compile.
 */
trait Waker:

  /**
   * Announce that this name may have something to act on.
   *
   * @param queue what became ready
   * @return noop
   */
  def ready(queue: QueueName): UIO[Unit]

  /**
   * Announce every name this instance knows of — the listener's error path, where a read may have stepped
   * over entries it cannot enumerate.
   *
   * @return noop
   */
  def readyAll: UIO[Unit]
