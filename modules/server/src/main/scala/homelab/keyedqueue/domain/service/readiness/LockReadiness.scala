package homelab.keyedqueue.domain.service.readiness


import homelab.keyedqueue.domain.types.LockName
import zio.*


/**
 * A wake for every parked waiter on a name — the lock's counterpart to [[QueueReadiness]].
 *
 * A wake reaches the mailboxes subscribed at that moment and no others, so a waiter subscribes before it
 * enters. It reaches all of them, because grants go by ticket order and only the store knows whose turn it
 * is: every woken waiter asks, and only the head can win. A mailbox coalesces — two wakes while parked read
 * as one, which is sound because a waiter acts on what it finds rather than on the count.
 *
 * See `docs/architecture/readiness-and-wake.md`.
 *
 * @param waiting lock → the mailboxes of its parked waiters
 */
final class LockReadiness(waiting: Ref[Map[LockName, Set[Queue[Unit]]]]):

  /**
   * Wake every waiter parked on this lock.
   *
   * Reaches the mailboxes subscribed at this moment and no others — a wake nobody is waiting for is
   * dropped, which the lock can afford because a waiter subscribes before it enters.
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
   * What a parked waiter holds: something to wait on, and nothing else.
   *
   * A wake while nobody waits is kept for the next await, and two wakes read as one — a waiter acts on
   * what it finds when it looks, not on how many times it was told.
   *
   * @param mailbox where this waiter's wakes land
   */
  final class Signal(mailbox: Queue[Unit]):

    /**
     * Wait for the next wake on this name.
     *
     * @return noop when one arrives
     */
    def await: UIO[Unit] = mailbox.take

  /**
   * An empty readiness.
   *
   * @return one with nobody waiting
   */
  def make: UIO[LockReadiness] =
    Ref.make(Map.empty[LockName, Set[Queue[Unit]]]).map(LockReadiness(_))
