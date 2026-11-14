package homelab.keyedqueue.domain.service.persistence


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.queue.{ Claim, Grant, ScheduledSubmission, Settlement, Submission }
import homelab.keyedqueue.domain.types.*
import zio.{ Chunk, Duration, IO }

import java.time.Instant


/**
 * The store the queue lives in: messages ordered per key, claims leased, and a sweep that repairs what a
 * death left behind.
 *
 * Deliberately says nothing about Redis. Every operation here is one atomic step in the substrate, which is
 * what lets a second implementation exist without the layer above noticing: a Postgres variant would do with
 * `SKIP LOCKED` and a `claimed_until` column what this one does with a script and a deadline set.
 *
 * It speaks in messages, and cargo stays opaque: only a message's key is structural, because ordering is
 * defined by it. The payload is moved unread.
 */
trait QueueStore:

  /**
   * Accept a message for a key, and make the key claimable if nothing is working it.
   *
   * The key is the message's own, so a message cannot be filed under a key that disagrees with it.
   *
   * @param submission where the message goes, and the message
   * @return the key's queue depth after the append; aborts with an `AdapterError` if the store fails
   */
  def enqueue(submission: Submission): IO[ApplicationError.AdapterError, Long]

  /**
   * Accept a message to be appended to its key's line once its delay has elapsed.
   *
   * Until then it waits outside the line, and nothing claims it. A message with the same id already scheduled
   * for the same key wins: this one is ignored, and the answer says so. An id is not compared with the key's
   * queued messages here, only when the message is moved: then, as for any enqueue, an id still queued for the
   * key means the scheduled copy is discarded.
   *
   * @param scheduled where the message goes, the message, and how long until it is due
   * @return whether it was recorded and how long until the recorded schedule is due, on the store's clock;
   *         aborts with an `AdapterError` if the store fails
   */
  def schedule(scheduled: ScheduledSubmission): IO[ApplicationError.AdapterError, QueueStore.Scheduled]

  /**
   * Append every scheduled message that has fallen due to its key's line, as an enqueue would.
   *
   * Idempotent, so every instance can run it without coordination: one moves an entry, the others find it
   * gone. A message whose id is still queued for its key is discarded rather than appended (D5).
   *
   * @param limit the most entries to move per part of the store in one call; a full call says so, and is
   *              repeated
   * @return what it moved, when the next entry falls due, and how many parts could not be reached; aborts with an
   *         `AdapterError` only when no part could be
   */
  def moveDue(limit: Int): IO[ApplicationError.AdapterError, QueueStore.Moved]

  /**
   * Claim whatever is claimable now, and answer at once.
   *
   * An empty answer means nothing was claimable at the moment it was asked, not that there is nothing
   * coming. Waiting for a queue to become worth another look is
   * [[homelab.keyedqueue.domain.service.readiness.QueueReadiness]]'s, not a store's.
   *
   * @param queue the queue to claim from
   * @param batch the most to take
   * @return the claim, or `None` when nothing was claimable; aborts with an `AdapterError` when the store
   *         fails
   */
  def attemptClaim(queue: QueueName, batch: Int): IO[ApplicationError.AdapterError, Option[Grant]]

  /**
   * Report what happened to some of what a claim owns.
   *
   * An id the claim does not own is ignored rather than refused, which is what makes a retried settle
   * harmless: settling removes the id from what the claim owns, and removing it again finds nothing.
   *
   * @param settlement the claim, what became of the messages it names, and any backoff
   * @return true when applied, false when the claim had already been revoked; aborts with an
   *         `AdapterError` if
   *         the store fails
   */
  def settle(settlement: Settlement): IO[ApplicationError.AdapterError, Boolean]

  /**
   * Push the deadline forward on the claims still held, and say which are gone.
   *
   * @param claims everything the caller believes it holds; may span queues
   * @return the new deadline, and the claims that were not renewed because they had been revoked; aborts
   *         with `ApplicationError` if the store fails
   */
  def renew(claims: Chunk[Claim]): IO[ApplicationError.AdapterError, (Instant, Chunk[Claim])]

  /**
   * How long a lease runs, which every grant and every renewal is written with.
   *
   * A deadline alone is a reading of this store's clock, so it is this that a consumer can time its
   * heartbeats by.
   *
   * @return the span
   */
  def leaseTtl: Duration

  /**
   * Repair what a death left behind: revoke lapsed claims, so their keys are claimable again.
   *
   * Idempotent, so every instance can run it without coordination.
   *
   * @param queue the queue to sweep
   * @param limit the most entries to handle per sweep
   * @return what it repaired, for logging and metrics; aborts with an `AdapterError` if the store fails
   */
  def sweep(queue: QueueName, limit: Int): IO[ApplicationError.AdapterError, QueueStore.Swept]

  /**
   * Make claimable again every key whose backoff has ended: a nack's wait holds back its key (C6), and this
   * puts it back.
   *
   * Idempotent, so every instance can run it without coordination. A key still held by its claim is not made
   * claimable here: the claim's end does it, since its backoff is over.
   *
   * @param limit the most backoffs to release per part of the store in one call; a full call says so, and is
   *              repeated
   * @return how many were released, when the next backoff ends, and how many parts could not be reached; aborts
   *         with an `AdapterError` only when no part could be
   */
  def releaseDue(limit: Int): IO[ApplicationError.AdapterError, QueueStore.Released]


object QueueStore:

  /**
   * What a schedule recorded.
   *
   * @param scheduled false when the id was already scheduled for the key, so the request was ignored
   * @param dueIn how long until the recorded schedule is due, on the store's clock
   */
  final case class Scheduled(scheduled: Boolean, dueIn: Duration)

  /**
   * What one move of due messages did.
   *
   * @param moved how many due messages were appended to their key's line
   * @param discarded how many were dropped because their id was still queued for the key
   * @param full whether a part of the store hit the limit, so more may be due right now
   * @param nextDueIn how long until the next scheduled message falls due, on the store's clock; absent when
   *                  nothing is scheduled
   * @param failed how many parts of the store could not be reached: the rest were still moved, and their next
   *               due time is still reported, so one unreachable part does not hold the others back
   */
  final case class Moved(moved: Int, discarded: Int, full: Boolean, nextDueIn: Option[Duration], failed: Int)

  /**
   * What one sweep repaired.
   *
   * @param reclaimed keys whose holder went silent mid-handler
   */
  final case class Swept(reclaimed: Chunk[MessageKey]):

    /** True when nothing needed repairing, which is the normal case and not worth logging. */
    def isEmpty: Boolean = reclaimed.isEmpty

  /**
   * What one release of ended backoffs did.
   *
   * @param released how many held-back keys had their backoff end
   * @param full whether a part of the store hit the limit, so more may have ended right now
   * @param nextDueIn how long until the next backoff ends, on the store's clock; absent when none is pending
   * @param failed how many parts of the store could not be reached: the rest were still released, and their next
   *               end is still reported
   */
  final case class Released(released: Int, full: Boolean, nextDueIn: Option[Duration], failed: Int)
