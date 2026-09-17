package homelab.keyedqueue.domain.service.usecase.queue


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.queue.{ Claim, Grant, Settlement, Submission }
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.QueueName
import zio.*

import java.time.Instant


/**
 * The queue store in memory, as far as the waiting needs one: it hands out the grants it was given, oldest
 * first, and answers the rest without recording anything.
 *
 * A claim finds a grant when one is pending and finds nothing otherwise, which is the only distinction the
 * waiting turns on. Nothing here reaches a substrate, so a spec over it runs without a container. It keeps
 * no messages, ownership or leases: a spec about those belongs against the real store.
 *
 * @param pending the grants still to be handed out
 */
final class InMemoryQueueStore(pending: Ref[Chunk[Grant]]) extends QueueStore:

  /**
   * Make a grant available to the next claim.
   *
   * @param grant what the next claim should find
   * @return noop
   */
  def hasWork(grant: Grant): UIO[Unit] = pending.update(_ :+ grant)

  /**
   * Take the oldest pending grant, if there is one.
   *
   * @param queue the queue asked about, which this store does not inspect
   * @param batch the most to take, which this store does not inspect
   * @return the grant, or `None` when none is pending
   */
  override def attemptClaim(queue: QueueName, batch: Int): IO[ApplicationError.AdapterError, Option[Grant]] =
    pending.modify(queued => (queued.headOption, queued.drop(1)))

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
  def make: UIO[InMemoryQueueStore] = Ref.make(Chunk.empty[Grant]).map(InMemoryQueueStore(_))
