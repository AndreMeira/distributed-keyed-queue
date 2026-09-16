package homelab.keyedqueue.domain.service.readiness


import homelab.keyedqueue.domain.types.QueueName
import zio.*


/**
 * One readiness token per queue: offered when a key becomes claimable, taken by the consumer that acts on
 * it.
 *
 * A coordination primitive, not a queue — it carries no work and knows nothing about Redis. A token says
 * "there may be something to claim" and the caller does the claiming, so the claim stays in the fiber that
 * will do the work. A token offered with nobody waiting is kept for the next look, and a token is a hint
 * that may be wrong rather than a promise: every path that could swallow one puts one back.
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
   * Wait for this queue's token for at most `patience`, and run `claim` when one arrives.
   *
   * The claim runs inside the wait: a token is never handed back to the caller, so it cannot be held as a
   * value by a fiber that dies with it. A look that finds work hands the token onward; a look that finds
   * nothing keeps it, which is what ends the chain.
   *
   * @param queue the queue to wait on
   * @param patience the longest to wait for a token
   * @param onReady what to do when one arrives; `None` means it looked and found nothing
   * @tparam E what `claim` aborts with
   * @tparam A what `claim` produces
   * @return `claim`'s answer, or `None` when nothing became ready in time; aborts with `E` when `claim` does
   */
  def awaitReady[E, A](queue: QueueName, patience: Duration)(onReady: IO[E, Option[A]]): IO[E, Option[A]] =
    buffer(queue).flatMap: found =>
      // Uninterruptible except where restored, so a token cannot be taken and then dropped in the gap
      // before its recovery is installed: interruption there would run neither handler.
      ZIO.uninterruptibleMask: restore =>
        for
          // Recovers a token the interruption would otherwise swallow. Attached outside the timeout on
          // purpose: when the timeout discards an element the take itself was never interrupted, so a
          // finalizer on the take never runs.
          ready  <- restore(found.take.timeout(patience)).onInterrupt(found.offer(()))
          // Flattened here, where the nesting is created: the outer `Option` says whether the claim ran,
          // the inner what it found, and conflating the two is how a fruitless look ends up handing the
          // token on and consumers spin.
          result <- restore(onReady.when(ready.isDefined).map(_.flatten)).onExit {
                      case Exit.Success(None) => ZIO.unit        // No work, make the next claim wait
                      case _                  => found.offer(()) // Work or failure, do not make it wait
                    }
          // The take gave up. It may have given up holding an element, which is unknowable, so put one back.
          _      <- found.offer(()).when(ready.isEmpty)
        yield result

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
   * An empty registry, with no queues yet.
   *
   * @return the readiness
   */
  def make: UIO[QueueReadiness] =
    Ref.make(Map.empty[QueueName, Queue[Unit]]).map(QueueReadiness(_))
