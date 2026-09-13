package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.LockName
import zio.*


/**
 * A wake for every parked waiter on a name — the lock's counterpart to [[Readiness]].
 *
 * '''As a [[Waker]]: a wake is dropped.''' It reaches the mailboxes subscribed at that moment and no
 * others. Safe here only because a waiter subscribes before it enters, so no release falls into a gap.
 *
 * '''Everyone wakes, because only the store knows whose turn it is.''' A fair lock grants by ticket order,
 * so waking one arbitrary waiter would as likely wake the wrong one — and the token it consumed would never
 * reach the head. Waking all costs one grant attempt per local waiter per event, and buys the property the
 * tickets exist for: the head cannot be starved by its neighbours.
 *
 * '''Subscribe first, then look.''' A subscription is a mailbox: wakes land in it from the moment it is
 * added, including while its owner is mid-look, so there is no gap in which a release can slip by unseen.
 * The mailbox coalesces — two wakes while parked read as one, which is sound because a waiter acts on what
 * it finds, not on the count.
 *
 * @param waiting lock → the mailboxes of its parked waiters
 */
final class Broadcast(waiting: Ref[Map[Waker.Name, Set[Queue[Unit]]]]) extends Waker:

  /**
   * Read a name as a lock's, which is all this sink ever wakes.
   *
   * @param raw the name as the entry carried it
   * @return it, as a lock name
   */
  override def name(raw: String): Waker.Name = LockName(raw)

  /**
   * Wake every waiter parked on this lock.
   *
   * Reaches the mailboxes subscribed at this moment and no others — a wake nobody is waiting for is
   * dropped, which the lock can afford because a waiter subscribes before it enters.
   *
   * @param lock what came free
   * @return noop
   */
  override def ready(lock: Waker.Name): UIO[Unit] =
    waiting.get.flatMap(current => ZIO.foreachDiscard(current.getOrElse(lock, Set.empty))(_.offer(()).unit))

  /**
   * Wake every waiter this instance holds, whatever they wait on — the listener's error path.
   *
   * @return noop
   */
  override def readyAll: UIO[Unit] =
    waiting.get.flatMap(current => ZIO.foreachDiscard(current.keys)(ready))

  /**
   * A mailbox for this name's wakes, held for the life of the scope.
   *
   * @param lock the lock to be woken for
   * @return the mailbox; wakes land in it until the scope closes
   */
  def subscribe(lock: Waker.Name): ZIO[Scope, Nothing, Queue[Unit]] =
    ZIO.acquireRelease(
      Queue.sliding[Unit](1).tap(mailbox => waiting.update(joined(lock, mailbox)))
    )(mailbox => waiting.update(left(lock, mailbox)))

  /**
   * The map with this mailbox added under the name.
   *
   * @param lock the lock subscribed to
   * @param mailbox the mailbox arriving
   * @param current the map as it was
   * @return the map as it becomes
   */
  private def joined(
    lock: Waker.Name,
    mailbox: Queue[Unit],
  )(
    current: Map[Waker.Name, Set[Queue[Unit]]]
  ): Map[Waker.Name, Set[Queue[Unit]]] =
    current.updated(lock, current.getOrElse(lock, Set.empty) + mailbox)

  /**
   * The map with this mailbox removed, and the name dropped when it was the last.
   *
   * @param lock the lock subscribed to
   * @param mailbox the mailbox leaving
   * @param current the map as it was
   * @return the map as it becomes
   */
  private def left(
    lock: Waker.Name,
    mailbox: Queue[Unit],
  )(
    current: Map[Waker.Name, Set[Queue[Unit]]]
  ): Map[Waker.Name, Set[Queue[Unit]]] =
    val remaining = current.getOrElse(lock, Set.empty) - mailbox
    if remaining.isEmpty then current.removed(lock) else current.updated(lock, remaining)


object Broadcast:

  /**
   * An empty broadcast.
   *
   * @return one with nobody waiting
   */
  def make: UIO[Broadcast] =
    Ref.make(Map.empty[Waker.Name, Set[Queue[Unit]]]).map(Broadcast(_))
