package homelab.keyedqueue.domain.service.persistence


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.queue.{ Claim, Grant, Settlement, Submission }
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
   * Repair what a death or a backoff left behind: revoke lapsed claims, and release keys whose retry delay
   * has elapsed.
   *
   * Idempotent, so every instance can run it without coordination.
   *
   * @param queue the queue to sweep
   * @param limit the most entries to handle per sweep, per kind
   * @return what it repaired, for logging and metrics; aborts with an `AdapterError` if the store fails
   */
  def sweep(queue: QueueName, limit: Int): IO[ApplicationError.AdapterError, QueueStore.Swept]


object QueueStore:

  /**
   * What one sweep repaired.
   *
   * @param reclaimed keys whose holder went silent mid-handler
   * @param released keys whose retry backoff elapsed
   */
  final case class Swept(reclaimed: Chunk[MessageKey], released: Chunk[MessageKey]):

    /** True when nothing needed repairing, which is the normal case and not worth logging. */
    def isEmpty: Boolean = reclaimed.isEmpty && released.isEmpty
