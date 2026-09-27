package homelab.keyedqueue.domain.service.readiness


import homelab.keyedqueue.domain.types.QueueName
import zio.*


/**
 * One readiness token per queue: offered when a key becomes claimable, taken by the consumer that acts on
 * it.
 *
 * A coordination primitive, not a queue — it carries no work and knows nothing about Redis. A token says
 * "there may be something to claim"; what to do about one, and what becomes of it afterwards, is the
 * taker's. A token offered with nobody waiting is kept for the next look, and repeats collapse into one, so
 * a spare costs one look and cannot pile up.
 *
 * See `docs/architecture/readiness-and-wake.md`.
 *
 * @param queues queue → its token buffer, made on first use
 */
final class QueueReadiness(queues: Ref[Map[QueueName, Queue[Unit]]]):

  /**
   * Announce that a queue may have work.
   *
   * Repeated announcements with nobody waiting collapse into one, which is sound because a consumer claims
   * whatever it finds rather than the specific key it was told about.
   *
   * @param queue what became claimable
   * @return noop
   */
  def ready(queue: QueueName): UIO[Unit] =
    buffer(queue).flatMap(_.offer(())).unit

  /**
   * Announce every queue this instance knows of.
   *
   * For the listener's error path. A read that failed may have been away long enough for wake entries to
   * be trimmed past, and `XREAD` does not report having stepped over any — so after a failure the safe
   * assumption is that something was announced and missed. Fires on a detectable event, never on a healthy
   * read.
   *
   * @return noop
   */
  def readyAll: UIO[Unit] =
    queues.get.flatMap(current => ZIO.foreachDiscard(current.keys)(ready))

  /**
   * A signal for this queue's tokens.
   *
   * Every subscriber to a name shares its one buffer, so a token one takes another does not see. A taker
   * that wants the next subscriber to look offers the token back through [[ready]].
   *
   * @param queue the queue to take tokens for
   * @return the signal
   */
  def subscribe(queue: QueueName): UIO[QueueReadiness.Signal] =
    buffer(queue).map(QueueReadiness.Signal(_))

  /**
   * A queue's token buffer, made on first use.
   *
   * Read first, allocate only on a miss: the map is written once per name and read on every claim, so the
   * common path is a single `Ref.get`. Two callers racing on a new name both build a buffer; one wins the
   * update and the other takes the winner's, discarding its own.
   *
   * A new buffer starts with a token, so a queue nobody has announced still gets one look.
   *
   * @param name the queue whose buffer is wanted
   * @return the buffer
   */
  private def buffer(name: QueueName): UIO[Queue[Unit]] =
    for {
      // get or create the token queue
      queue <- queues.get.flatMap: known =>
                 known.get(name) match
                   case Some(q) => ZIO.succeed(q)
                   case None    => Queue.sliding[Unit](1).tap(_.offer(()))

      // get or install the token queue
      installed <- queues.modify: known =>
                     known.get(name) match
                       case Some(raced) => raced -> known
                       case None        => queue -> known.updated(name, queue)
    } yield installed


object QueueReadiness:

  /**
   * What a consumer waits on.
   *
   * @param buffer the queue's token buffer
   */
  final class Signal(buffer: Queue[Unit]):

    /**
     * Take a token, waiting at most `patience` for one.
     *
     * A wait that gives up may have taken a token it never saw — the timeout forks the take, and the fork
     * can win as the timeout fires — so one is put back on every give-up, and on an interruption. The buffer
     * holds one, so a spare costs one look and cannot pile up.
     *
     * @param patience the longest to wait
     * @return whether a token was taken; never fails
     */
    def await(patience: Duration): UIO[Boolean] =
      ZIO.uninterruptibleMask: restore =>
        restore(buffer.take.timeout(patience))
          .onInterrupt(buffer.offer(()))
          .flatMap:
            case Some(_) => ZIO.succeed(true)
            case None    => buffer.offer(()).as(false)

  /**
   * An empty registry, with no queues yet.
   *
   * @return the registry
   */
  def make: UIO[QueueReadiness] =
    Ref.make(Map.empty[QueueName, Queue[Unit]]).map(QueueReadiness(_))
