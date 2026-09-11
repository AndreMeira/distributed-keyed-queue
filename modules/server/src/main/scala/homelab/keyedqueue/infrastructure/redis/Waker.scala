package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.QueueName
import zio.*


/**
 * What a wake can be delivered to: the face [[WakeListener]] routes entries through.
 *
 * Two shapes stand behind it, and the difference is who gets woken. [[Readiness]] hands one token to one
 * consumer — right for the queue, where any woken consumer can claim whatever is ready. [[Broadcast]] wakes
 * every parked waiter — right for the lock, where only the head ticket may proceed and everyone else must
 * still look to learn their new wait.
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
