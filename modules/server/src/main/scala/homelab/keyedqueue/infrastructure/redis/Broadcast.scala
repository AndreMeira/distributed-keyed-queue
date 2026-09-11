package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.QueueName
import zio.*


/**
 * A wake for every parked waiter on a name — the lock's counterpart to [[Readiness]].
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
 * @param waiting name → the mailboxes of its parked waiters
 */
final class Broadcast(waiting: Ref[Map[QueueName, Set[Queue[Unit]]]]) extends Waker:

  override def ready(queue: QueueName): UIO[Unit] =
    waiting.get.flatMap(current => ZIO.foreachDiscard(current.getOrElse(queue, Set.empty))(_.offer(()).unit))

  override def readyAll: UIO[Unit] =
    waiting.get.flatMap(current => ZIO.foreachDiscard(current.keys)(ready))

  /**
   * A mailbox for this name's wakes, held for the life of the scope.
   *
   * @param queue the name to be woken for
   * @return the mailbox; wakes land in it until the scope closes
   */
  def subscribe(queue: QueueName): ZIO[Scope, Nothing, Queue[Unit]] =
    ZIO.acquireRelease(
      Queue.sliding[Unit](1).tap(mailbox => waiting.update(joined(queue, mailbox)))
    )(mailbox => waiting.update(left(queue, mailbox)))

  /**
   * The map with this mailbox added under the name.
   *
   * @param queue the name subscribed to
   * @param mailbox the mailbox arriving
   * @param current the map as it was
   * @return the map as it becomes
   */
  private def joined(
    queue: QueueName,
    mailbox: Queue[Unit],
  )(
    current: Map[QueueName, Set[Queue[Unit]]]
  ): Map[QueueName, Set[Queue[Unit]]] =
    current.updated(queue, current.getOrElse(queue, Set.empty) + mailbox)

  /**
   * The map with this mailbox removed, and the name dropped when it was the last.
   *
   * @param queue the name subscribed to
   * @param mailbox the mailbox leaving
   * @param current the map as it was
   * @return the map as it becomes
   */
  private def left(
    queue: QueueName,
    mailbox: Queue[Unit],
  )(
    current: Map[QueueName, Set[Queue[Unit]]]
  ): Map[QueueName, Set[Queue[Unit]]] =
    val remaining = current.getOrElse(queue, Set.empty) - mailbox
    if remaining.isEmpty then current.removed(queue) else current.updated(queue, remaining)


object Broadcast:

  /**
   * An empty broadcast.
   *
   * @return one with nobody waiting
   */
  def make: UIO[Broadcast] =
    Ref.make(Map.empty[QueueName, Set[Queue[Unit]]]).map(Broadcast(_))
