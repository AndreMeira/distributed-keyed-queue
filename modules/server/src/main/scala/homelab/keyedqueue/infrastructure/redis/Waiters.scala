package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.QueueName
import zio.*


/**
 * A bell per queue, rung by whoever makes a key claimable and heard by every consumer holding it.
 *
 * A coordination primitive, not a queue: it carries no work and knows nothing about Redis. A ring means
 * "look again", and the caller does the looking — which is what keeps the claim in the fiber that will do
 * the work.
 *
 * '''Nothing is consumed, which is the whole point.''' A ring is a broadcast to everyone holding the
 * current bell, not a token handed to one of them. Nobody can take a ring away from anybody else, so there
 * is no handover to get wrong: a caller that times out, is interrupted, or dies mid-claim removes nothing
 * from the system. That is what a registry of one-shot handovers cannot offer, because "you were given a
 * wake" and "you gave up" have to be decided as one atomic step, and every way of doing that has a window
 * where the wake dies with its taker.
 *
 * '''The price is a look each.''' Every consumer waiting on the queue wakes and attempts a claim, and one
 * wins. Redundant attempts are the currency this design pays in — the same currency it already pays across
 * instances, where every instance hears every entry.
 *
 * '''Subscribe, then look, then wait.''' [[subscribe]] takes the current bell; a ring installs a fresh one
 * and completes the old. A caller that takes its bell '''before''' its claim attempt cannot miss a ring
 * that arrives during the attempt: the ring completes the bell it is already holding. Get that order
 * wrong and a consumer sleeps beside work — which is why the order lives here, in the doc, and in
 * [[RedisQueueStore]]'s loop.
 *
 * @param bells queue → the bell now current, made on first use
 */
final class Waiters(bells: Ref.Synchronized[Map[QueueName, Promise[Nothing, Unit]]]):

  /**
   * Take the bell now current for a queue, to be awaited after a claim attempt finds nothing.
   *
   * @param queue the queue to listen to
   * @return the bell to await
   */
  def subscribe(queue: QueueName): UIO[Waiters.Bell] =
    bells.modifyZIO: current =>
      current.get(queue) match
        case Some(bell) => ZIO.succeed(Waiters.Bell(bell) -> current)
        case None       => Promise.make[Nothing, Unit].map(bell => Waiters.Bell(bell) -> current.updated(queue, bell))

  /**
   * Ring a queue's bell: wake every consumer holding it, and install a fresh one for the next round.
   *
   * A ring with nobody listening is not remembered, and does not need to be: a consumer subscribes before
   * it claims, so anything a ring announced is found by the claim of whoever comes next.
   *
   * @param queue what became claimable
   * @return noop
   */
  def wake(queue: QueueName): UIO[Unit] =
    bells
      .modifyZIO: current =>
        Promise
          .make[Nothing, Unit]
          .map(fresh => current.get(queue) -> current.updated(queue, fresh))
      .flatMap(rung => ZIO.foreachDiscard(rung)(_.succeed(())))


object Waiters:

  /**
   * One round's bell: completed when the queue is rung, and replaced by the next.
   *
   * @param promise what a ring completes
   */
  final class Bell(promise: Promise[Nothing, Unit]):

    /**
     * Wait for this bell to ring, for at most `patience`.
     *
     * Interruption and expiry both take nothing away: the bell belongs to the round, not to the caller,
     * so whoever else is holding it is unaffected.
     *
     * @param patience the longest to wait
     * @return whether it rang
     */
    def await(patience: Duration): UIO[Boolean] = promise.await.timeout(patience).map(_.isDefined)

    /**
     * Whether this bell has rung, without waiting.
     *
     * For tests and metrics.
     *
     * @return whether it rang
     */
    def rang: UIO[Boolean] = promise.isDone

  /**
   * An empty registry, with no bells yet.
   *
   * @return the waiters
   */
  def make: UIO[Waiters] =
    Ref.Synchronized.make(Map.empty[QueueName, Promise[Nothing, Unit]]).map(Waiters(_))
