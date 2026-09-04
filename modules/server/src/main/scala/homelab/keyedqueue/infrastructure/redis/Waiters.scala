package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.QueueName
import zio.*


/**
 * One signal per queue, raised when a key there becomes claimable and awaited by every consumer waiting on
 * it.
 *
 * A coordination primitive, not a queue: it carries no work and knows nothing about Redis. A signal means
 * "look again", and the caller does the looking — which is what keeps the claim in the fiber that will do
 * the work.
 *
 * '''Nothing is consumed, which is the whole point.''' A signal reaches everyone holding it; it is not a
 * token handed to one of them. Nobody can take a wake away from anybody else, so there is no handover to
 * get wrong: a caller that times out, is interrupted, or dies mid-claim removes nothing from the system.
 * That is what a registry of one-shot handovers cannot offer, because "you were given a wake" and "you gave
 * up" have to be decided as one atomic step, and every way of doing that leaves a window where the wake
 * dies with its taker.
 *
 * '''The price is a look each.''' Every consumer waiting on the queue wakes and attempts a claim, and one
 * wins. Redundant attempts are the currency this design pays in — the same currency it already pays across
 * instances, where every instance reads every wake.
 *
 * '''Subscribe, then look, then wait.''' [[subscribe]] takes the signal now current; raising one replaces
 * it with a fresh signal and completes the old. A caller that takes its signal '''before''' its claim
 * attempt cannot miss a wake that arrives during the attempt: it completes the signal the caller is already
 * holding. Get that order wrong and a consumer sleeps beside work — which is why the order lives here, in
 * the doc, and in [[RedisQueueStore]]'s loop.
 *
 * @param signals queue → the signal now current, made on first use
 */
final class Waiters(signals: Ref.Synchronized[Map[QueueName, Promise[Nothing, Unit]]]):

  /**
   * Take the signal now current for a queue, to be awaited after a claim attempt finds nothing.
   *
   * @param queue the queue to listen to
   * @return the signal to await
   */
  def subscribe(queue: QueueName): UIO[Waiters.Signal] =
    signals.modifyZIO: current =>
      current.get(queue) match
        case Some(signal) => ZIO.succeed(Waiters.Signal(signal) -> current)
        case None         =>
          Promise.make[Nothing, Unit].map(signal => Waiters.Signal(signal) -> current.updated(queue, signal))

  /**
   * Raise a queue's signal: wake every consumer holding it, and put a fresh one in its place.
   *
   * A wake with nobody listening is not remembered, and does not need to be: a consumer subscribes before
   * it claims, so anything the wake announced is found by the claim of whoever comes next.
   *
   * @param queue what became claimable
   * @return noop
   */
  def raise(queue: QueueName): UIO[Unit] =
    signals
      .modifyZIO: current =>
        Promise
          .make[Nothing, Unit]
          .map(fresh => current.get(queue) -> current.updated(queue, fresh))
      .flatMap(raised => ZIO.foreachDiscard(raised)(_.succeed(())))

  /**
   * Raise every signal this instance holds.
   *
   * For the listener's error path. A read that failed may have been away long enough for entries to be
   * trimmed past — and `XREAD` does not report having stepped over any, it simply hands back what is left —
   * so after a failure the safe assumption is that something was announced and missed. Raising everything
   * costs one look per waiting consumer, once, after a failure that already means trouble; leaving them
   * asleep costs whatever their patience is.
   *
   * Deliberately not a periodic sweep: it fires on a detectable event, never on a healthy read.
   *
   * @return noop
   */
  def raiseAll: UIO[Unit] =
    signals.get.flatMap(current => ZIO.foreachDiscard(current.keys)(raise))


object Waiters:

  /**
   * One round's signal: completed when the queue is next announced, and replaced by the next.
   *
   * @param promise what raising the signal completes
   */
  final class Signal(promise: Promise[Nothing, Unit]):

    /**
     * Wait for this signal, for at most `patience`.
     *
     * Interruption and expiry both take nothing away: the signal belongs to the round, not to the caller,
     * so whoever else is holding it is unaffected.
     *
     * @param patience the longest to wait
     * @return whether it was raised
     */
    def await(patience: Duration): UIO[Boolean] = promise.await.timeout(patience).map(_.isDefined)

    /**
     * Whether this signal has been raised, without waiting.
     *
     * For tests and metrics.
     *
     * @return whether it was raised
     */
    def raised: UIO[Boolean] = promise.isDone

  /**
   * An empty registry, with no signals yet.
   *
   * @return the waiters
   */
  def make: UIO[Waiters] =
    Ref.Synchronized.make(Map.empty[QueueName, Promise[Nothing, Unit]]).map(Waiters(_))
