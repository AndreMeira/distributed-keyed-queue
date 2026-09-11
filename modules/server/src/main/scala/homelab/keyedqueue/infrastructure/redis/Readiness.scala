package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.QueueName
import zio.*


/**
 * One readiness token per queue: offered when a key becomes claimable, taken by the consumer that acts on
 * it.
 *
 * A coordination primitive, not a queue: it carries no work and knows nothing about Redis. A token means
 * "there may be something to claim", and the caller does the claiming — which is what keeps the claim in
 * the fiber that will do the work.
 *
 * '''One token, one consumer.''' This is the point of the design. A broadcast wakes every consumer parked
 * on a queue so that one of them can win a claim and the rest waste a round trip; taking a token wakes
 * exactly one. What replaces the broadcast is the hand-on in [[awaitReady]]: a consumer that finds work offers
 * the token onwards, so a burst drains one consumer at a time and stops on the first fruitless look.
 *
 * '''A token is a hint that may be wrong, never a promise that may be lost.''' Every path that could
 * swallow a token puts one back — the patience elapsing, the effect failing, the caller being interrupted.
 * Those recoveries are unconditional, because a `Queue` cannot report whether *this* taker received the
 * element, so it is not knowable whether there is anything to put back. That is safe only because the
 * buffer holds one token: a spurious offer costs one wasted look and cannot accumulate, while a lost token
 * costs a queue going quiet with work sitting in it. The design leans on that asymmetry throughout.
 *
 * '''Order does not matter here, and that is a real simplification.''' The buffer remembers, so a token
 * offered while a consumer is mid-claim waits for its next take. A promise-based signal had to be
 * subscribed to *before* looking or the wake was lost — a constraint that was load-bearing, untestable and
 * easy to break.
 *
 * @param queues queue → its token buffer, made on first use
 */
final class Readiness(queues: Ref[Map[QueueName, Queue[Unit]]]) extends Waker:

  /**
   * Announce that a queue may have work.
   *
   * Repeated announcements with nobody waiting collapse into one, which is sound because a consumer claims
   * whatever it finds rather than the specific key it was told about.
   *
   * @param queue what became claimable
   * @return noop
   */
  override def ready(queue: QueueName): UIO[Unit] =
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
  override def readyAll: UIO[Unit] =
    queues.get.flatMap(current => ZIO.foreachDiscard(current.keys)(ready))

  /**
   * Wait for this queue's token for at most `patience`, and run `claim` when one arrives.
   *
   * The claim runs *inside* rather than the token being handed back to the caller, and that is what makes
   * the handover safe: a token cannot be held as a value by a fiber that then dies with it.
   *
   * Answers what `claim` answered, or `None` when the patience elapsed without a token.
   *
   * @param queue the queue to wait on
   * @param patience the longest to wait for a token
   * @param claim what to do when one arrives; `None` means it looked and found nothing
   * @tparam E what `claim` aborts with
   * @tparam A what `claim` produces
   * @return `claim`'s answer, or `None` when nothing became ready in time; aborts with `E` when `claim` does
   */
  def awaitReady[E, A](queue: QueueName, patience: Duration)(claim: IO[E, Option[A]]): IO[E, Option[A]] =
    buffer(queue).flatMap: found =>
      // Uninterruptible except where restored, so a token cannot be taken and then dropped in the gap
      // before its recovery is installed: interruption there would run neither handler.
      ZIO.uninterruptibleMask: restore =>
        for
          // Recovers a token the interruption would otherwise swallow. Attached outside the timeout on
          // purpose: when the timeout discards an element the take itself was never interrupted, so a
          // finaliser on the take never runs.
          ready  <- restore(found.take.timeout(patience)).onInterrupt(found.offer(()))
          // Flattened here, where the nesting is created: the outer `Option` says whether the claim ran,
          // the inner what it found, and conflating the two is how a fruitless look ends up handing the
          // token on and consumers spin.
          result <- restore(claim.when(ready.isDefined).map(_.flatten)).onExit {
                      case Exit.Success(None) => ZIO.unit        // No work, make the next claim wait
                      case _                  => found.offer(()) // Work or failure, do not make it wait
                    }
          // The take gave up. It may have given up holding an element, which is unknowable, so put one back.
          _      <- found.offer(()).when(ready.isEmpty)
        yield result

  /**
   * A queue's token buffer, made on first use.
   *
   * '''Read first, and allocate only on a miss.''' The map is written once per queue name and read on
   * every claim, so the common path is a single `Ref.get` with no allocation and no lock. The re-check
   * inside `modify` is what makes that safe: two callers racing on a new name both build a buffer, one
   * wins the update and the other takes the winner's, discarding its own.
   *
   * '''A new buffer starts with a token.''' A queue nobody has announced still deserves one look — after a
   * restart the wake stream is positioned at its end, so work already sitting in `ready` would otherwise
   * never be announced and a consumer would wait out its patience beside it.
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


object Readiness:

  /**
   * An empty registry, with no queues yet.
   *
   * @return the readiness
   */
  def make: UIO[Readiness] = Ref.make(Map.empty[QueueName, Queue[Unit]]).map(Readiness(_))
