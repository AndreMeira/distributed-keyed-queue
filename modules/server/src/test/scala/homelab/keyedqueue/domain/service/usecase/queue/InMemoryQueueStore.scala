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
 * @param leaseTtl the span it reports its leases run for
 */
final class InMemoryQueueStore(
  pending: Ref[Chunk[Grant]],
  looks: Ref[Int],
  schedules: Ref[Chunk[ScheduledSubmission]],
  moves: Ref[Chunk[Either[ApplicationError.AdapterError, QueueStore.Moved]]],
  moveCalls: Ref[Int],
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
   * Record the schedule, and answer that it was recorded with its own delay.
   *
   * @param scheduled what was asked for
   * @return recorded, due in its delay
   */
  override def schedule(scheduled: ScheduledSubmission): IO[ApplicationError.AdapterError, QueueStore.Scheduled] =
    schedules.update(_ :+ scheduled).as(QueueStore.Scheduled(scheduled = true, dueIn = scheduled.delay))

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

  override def settle(settlement: Settlement): IO[ApplicationError.AdapterError, Boolean] = ZIO.succeed(true)

  override def renew(claims: Chunk[Claim]): IO[ApplicationError.AdapterError, (Instant, Chunk[Claim])] =
    ZIO.succeed(Instant.EPOCH -> Chunk.empty)

  override def sweep(queue: QueueName, limit: Int): IO[ApplicationError.AdapterError, QueueStore.Swept] =
    ZIO.succeed(QueueStore.Swept(Chunk.empty, Chunk.empty))


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
    yield InMemoryQueueStore(pending, looks, schedules, moves, moveCalls)
