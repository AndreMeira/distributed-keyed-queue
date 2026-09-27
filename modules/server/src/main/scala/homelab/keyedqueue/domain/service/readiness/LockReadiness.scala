package homelab.keyedqueue.domain.service.readiness


import homelab.keyedqueue.domain.types.LockName
import zio.*


/**
 * Announces that a lock came free, and hands a waiter the signal that hears it.
 *
 * A waiter receives only the wakes sent after it subscribed; earlier ones do not reach it. Its mailbox then
 * holds one wake until taken, so a waiter that is not looking misses nothing, and several wakes before it
 * looks read as one.
 *
 * See `docs/architecture/readiness-and-wake.md` for why a wake goes to every waiter.
 *
 * @param waiting lock → the mailboxes of its parked waiters
 */
final class LockReadiness(waiting: Ref[Map[LockName, Set[Queue[Unit]]]]):

  /**
   * Wake every waiter parked on this lock.
   *
   * Reaches the waiters subscribed at the moment of the call. A wake sent while none are is not kept for a
   * later subscriber.
   *
   * @param lock what came free
   * @return noop
   */
  def ready(lock: LockName): UIO[Unit] =
    waiting.get.flatMap: current =>
      val queues = current.getOrElse(lock, Set.empty)
      ZIO.foreachDiscard(queues)(queue => queue.offer(()))

  /**
   * Wake every waiter this instance holds, whatever they wait on — the listener's error path.
   *
   * @return noop
   */
  def readyAll: UIO[Unit] =
    waiting.get.flatMap: current =>
      ZIO.foreachDiscard(current.keys)(ready)

  /**
   * A signal for this name's wakes, good for the life of the scope.
   *
   * @param lock the lock to be woken for
   * @return the signal; wakes reach it until the scope closes
   */
  def subscribe(lock: LockName): ZIO[Scope, Nothing, LockReadiness.Signal] =
    ZIO
      .acquireRelease(addSubscriber(lock))(removeSubscriber(lock, _))
      .map(LockReadiness.Signal(_))

  /**
   * Open a mailbox for this lock and put it among those a wake reaches.
   *
   * @param lock the lock to be woken for
   * @return the mailbox, already subscribed
   */
  private def addSubscriber(lock: LockName): UIO[Queue[Unit]] =
    for
      mailbox <- Queue.sliding[Unit](1)
      _       <- waiting.update: current =>
                   val subscribers = current.getOrElse(lock, Set.empty) + mailbox
                   current.updated(lock, subscribers)
    yield mailbox

  /**
   * Take this mailbox out of the lock's subscribers, and the name with it once it holds none.
   *
   * @param lock the lock subscribed to
   * @param mailbox the mailbox leaving
   * @return noop
   */
  private def removeSubscriber(lock: LockName, mailbox: Queue[Unit]): UIO[Unit] =
    waiting.update: current =>
      val remaining = current.getOrElse(lock, Set.empty) - mailbox
      if remaining.isEmpty then current.removed(lock) else current.updated(lock, remaining)


object LockReadiness:

  /**
   * What a parked waiter waits on.
   *
   * @param mailbox where this waiter's wakes land
   */
  final class Signal(mailbox: Queue[Unit]):

    /**
     * Wait for the next wake on this name.
     *
     * Returns at once when a wake arrived since the last call; several that arrived read as one.
     *
     * @return noop when a wake arrives
     */
    def await: UIO[Unit] = mailbox.take

  /**
   * An empty readiness.
   *
   * @return one with nobody waiting
   */
  def make: UIO[LockReadiness] =
    Ref.make(Map.empty[LockName, Set[Queue[Unit]]]).map(LockReadiness(_))
