package homelab.keyedqueue.domain.service.usecase.queue


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.queue.{ Claim, Grant, ScheduledSubmission, Settlement, Submission }
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.QueueName
import zio.*

import java.time.Instant


/**
 * The queue store in memory, as far as the waiting and the scheduling need one: it hands out the grants it was
 * given, oldest first, records the schedules it is asked for, answers moves with the replies it was given, and
 * answers the rest without recording anything.
 *
 * A claim finds a grant when one is pending and finds nothing otherwise, which is the only distinction the
 * waiting turns on. Nothing here reaches a substrate, so a spec over it runs without a container. It keeps
 * no messages, ownership or leases: a spec about those belongs against the real store.
 *
 * @param pending the grants still to be handed out
 * @param looks how many claims have been attempted
 * @param schedules the schedules recorded, oldest first
 * @param moves the replies the next moves answer with, in order; a move with none left answers that nothing
 *              moved and nothing is scheduled
 * @param moveCalls how many moves have been asked for
 * @param held when set, a schedule or a settle waits on this after recording, until a test releases it
 * @param releases the replies the next releases of ended backoffs answer with, in order; one with none left answers
 *                 that nothing ended and nothing is pending
 * @param releaseCalls how many releases have been asked for
 * @param settles the settlements applied, oldest first
 * @param leaseTtl the span it reports its leases run for
 */
final class InMemoryQueueStore(
  pending: Ref[Chunk[Grant]],
  looks: Ref[Int],
  schedules: Ref[Chunk[ScheduledSubmission]],
  moves: Ref[Chunk[Either[ApplicationError.AdapterError, QueueStore.Moved]]],
  moveCalls: Ref[Int],
  held: Ref[Option[Promise[Nothing, Unit]]],
  releases: Ref[Chunk[Either[ApplicationError.AdapterError, QueueStore.Released]]],
  releaseCalls: Ref[Int],
  settles: Ref[Chunk[Settlement]],
  val leaseTtl: Duration = 30.seconds,
) extends QueueStore:

  /**
   * Make a grant available to the next claim.
   *
   * @param grant what the next claim should find
   * @return noop
   */
  def hasWork(grant: Grant): UIO[Unit] = pending.update(_ :+ grant)

  /**
   * How many times a claim has been attempted, whatever it found.
   *
   * @return the count
   */
  def looked: UIO[Int] = looks.get

  /**
   * What the schedules asked for.
   *
   * @return the scheduled submissions, oldest first
   */
  def scheduled: UIO[Chunk[ScheduledSubmission]] = schedules.get

  /**
   * Queue up the reply the next move answers with.
   *
   * @param reply what the move answers: the store's failure, or what it moved
   * @return noop
   */
  def movesWith(reply: Either[ApplicationError.AdapterError, QueueStore.Moved]): UIO[Unit] = moves.update(_ :+ reply)

  /**
   * How many moves have been asked for.
   *
   * @return the count
   */
  def moved: UIO[Int] = moveCalls.get

  /**
   * Make every later schedule and settle wait, once recorded, until `release` completes: the moment a caller can be
   * cancelled after the store has written and before it has answered, or a slow reply.
   *
   * @param release what the held writes wait for
   * @return noop
   */
  def holdWritesUntil(release: Promise[Nothing, Unit]): UIO[Unit] = held.set(Some(release))

  /**
   * Record the schedule, wait if writes are held, and answer that it was recorded with its own delay.
   *
   * @param scheduled what was asked for
   * @return recorded, due in its delay
   */
  override def schedule(scheduled: ScheduledSubmission): IO[ApplicationError.AdapterError, QueueStore.Scheduled] =
    for
      _       <- schedules.update(_ :+ scheduled)
      release <- held.get
      _       <- waitFor(release)
    yield QueueStore.Scheduled(scheduled = true, dueIn = scheduled.delay)

  /**
   * Wait for a release, when there is one.
   *
   * @param release what to wait for, if schedules are held
   * @return noop once released, or at once when nothing is held
   */
  private def waitFor(release: Option[Promise[Nothing, Unit]]): UIO[Unit] =
    release match
      case Some(promise) => promise.await
      case None          => ZIO.unit

  /**
   * Answer with the next queued reply, or with nothing moved and nothing scheduled when none is left.
   *
   * @param limit the bound asked for, which this store does not inspect
   * @return the reply
   */
  override def moveDue(limit: Int): IO[ApplicationError.AdapterError, QueueStore.Moved] =
    for
      _     <- moveCalls.update(_ + 1)
      reply <- moves.modify(queued => (queued.headOption, queued.drop(1)))
      moved <- ZIO.fromEither(reply.getOrElse(Right(QueueStore.Moved(0, 0, full = false, nextDueIn = None, failed = 0))))
    yield moved

  /**
   * Take the oldest pending grant, if there is one.
   *
   * @param queue the queue asked about, which this store does not inspect
   * @param batch the most to take, which this store does not inspect
   * @return the grant, or `None` when none is pending
   */
  override def attemptClaim(queue: QueueName, batch: Int): IO[ApplicationError.AdapterError, Option[Grant]] =
    looks.update(_ + 1) *> pending.modify(queued => (queued.headOption, queued.drop(1)))

  // The operations below are not exercised by the specs that use this store; they answer without recording.

  override def enqueue(submission: Submission): IO[ApplicationError.AdapterError, Long] = ZIO.succeed(1L)

  /**
   * Record the settlement, wait if writes are held, and answer that it applied.
   *
   * @param settlement what was asked for
   * @return applied
   */
  override def settle(settlement: Settlement): IO[ApplicationError.AdapterError, Boolean] =
    for
      _       <- settles.update(_ :+ settlement)
      release <- held.get
      _       <- waitFor(release)
    yield true

  override def renew(claims: Chunk[Claim]): IO[ApplicationError.AdapterError, (Instant, Chunk[Claim])] =
    ZIO.succeed(Instant.EPOCH -> Chunk.empty)

  override def sweep(queue: QueueName, limit: Int): IO[ApplicationError.AdapterError, QueueStore.Swept] =
    ZIO.succeed(QueueStore.Swept(Chunk.empty))

  /**
   * Answer with the next queued reply, or with nothing ended and nothing pending when none is left.
   *
   * @param limit the bound asked for, which this store does not inspect
   * @return the reply
   */
  override def releaseDue(limit: Int): IO[ApplicationError.AdapterError, QueueStore.Released] =
    for
      _        <- releaseCalls.update(_ + 1)
      reply    <- releases.modify(queued => (queued.headOption, queued.drop(1)))
      released <- ZIO.fromEither(reply.getOrElse(Right(QueueStore.Released(0, full = false, nextDueIn = None, failed = 0))))
    yield released

  /**
   * Queue up the reply the next release of ended backoffs answers with.
   *
   * @param reply what the release answers: the store's failure, or what it released
   * @return noop
   */
  def releasesWith(reply: Either[ApplicationError.AdapterError, QueueStore.Released]): UIO[Unit] =
    releases.update(_ :+ reply)

  /**
   * How many releases of ended backoffs have been asked for.
   *
   * @return the count
   */
  def released: UIO[Int] = releaseCalls.get

  /**
   * The settlements applied.
   *
   * @return them, oldest first
   */
  def settled: UIO[Chunk[Settlement]] = settles.get


object InMemoryQueueStore:

  /**
   * A store with nothing pending.
   *
   * @return the store
   */
  def make: UIO[InMemoryQueueStore] =
    for
      pending   <- Ref.make(Chunk.empty[Grant])
      looks     <- Ref.make(0)
      schedules <- Ref.make(Chunk.empty[ScheduledSubmission])
      moves     <- Ref.make(Chunk.empty[Either[ApplicationError.AdapterError, QueueStore.Moved]])
      moveCalls <- Ref.make(0)
      held      <- Ref.make(Option.empty[Promise[Nothing, Unit]])
      releases  <- Ref.make(Chunk.empty[Either[ApplicationError.AdapterError, QueueStore.Released]])
      calls     <- Ref.make(0)
      settles   <- Ref.make(Chunk.empty[Settlement])
    yield InMemoryQueueStore(pending, looks, schedules, moves, moveCalls, held, releases, calls, settles)
