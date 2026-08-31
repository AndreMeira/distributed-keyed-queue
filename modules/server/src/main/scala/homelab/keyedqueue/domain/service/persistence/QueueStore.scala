package homelab.keyedqueue.domain.service.persistence


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ Claim, Claimed, Demand, Settlement, Submission }
import homelab.keyedqueue.domain.types.*
import zio.{ Chunk, Duration, IO }

import java.time.Instant


/**
 * The store the queue lives in: messages ordered per key, claims leased, and a sweep that repairs what a
 * death left behind.
 *
 * Deliberately says nothing about Redis. Every operation here is one atomic step in the substrate, which is
 * what lets a second implementation exist without the layer above noticing: a Postgres variant would do with
 * `SKIP LOCKED` and a `claimed_until` column what this one does with `BLMOVE` and a deadline set.
 *
 * '''It speaks in messages, and cargo stays opaque.''' How a message is serialised is this port's business,
 * not its caller's — the same way key layout is. What the store never does is look *inside* a message: only
 * its key is structural, because ordering is defined by it, and the payload is cargo it moves unread.
 */
trait QueueStore:

  /**
   * Accept a message for a key, and make the key claimable if nothing is working it.
   *
   * The key is the message's own, so a message cannot be filed under a key that disagrees with it.
   *
   * @param submission where the message goes, and the message
   * @return the key's queue depth after the append; aborts with `QueueError` if the store fails
   */
  def enqueue(submission: Submission): IO[QueueError, Long]

  /**
   * Wait for a key to become claimable, then take the oldest of its messages the demand allows for.
   *
   * Blocks the calling fiber, and with it a connection, which is why the blocking pool's size — not the
   * number of keys — is what bounds concurrent claims, and why this is not something to fork per request.
   *
   * One claim covers the whole batch: the key is what is owned, so nothing else may work any of these
   * messages until the claim ends, however many it turned out to contain.
   *
   * @param demand the queue to claim from, how long to wait, and the most to take
   * @return the claim, or `None` when nothing became claimable in time; aborts with `QueueError` if the
   *         store fails
   */
  def claim(demand: Demand): IO[QueueError, Option[Claimed]]

  /**
   * Report what happened to some of what a claim owns.
   *
   * An id the claim does not own is ignored rather than refused, which is what makes a retried settle
   * harmless: settling removes the id from what the claim owns, and removing it again finds nothing.
   *
   * @param settlement the claim, what became of the messages it names, and any backoff
   * @return true when applied, false when the claim had already been revoked; aborts with `QueueError` if
   *         the store fails
   */
  def settle(settlement: Settlement): IO[QueueError, Boolean]

  /**
   * Push the deadline forward on the claims still held, and say which are gone.
   *
   * @param claims everything the caller believes it holds; may span queues
   * @return the new deadline, and the claims that were not renewed because they had been revoked; aborts
   *         with `QueueError` if the store fails
   */
  def renew(claims: Chunk[Claim]): IO[QueueError, (Instant, Chunk[Claim])]

  /**
   * Repair what a death or a backoff left behind: revoke lapsed claims, recover the keys of workers that
   * died mid-claim, and release keys whose retry delay has elapsed.
   *
   * Idempotent, so every instance can run it without coordination.
   *
   * @param queue the queue to sweep
   * @param limit the most entries to handle per sweep, per kind
   * @return what it repaired, for logging and metrics; aborts with `QueueError` if the store fails
   */
  def sweep(queue: QueueName, limit: Int): IO[QueueError, QueueStore.Swept]


object QueueStore:

  /**
   * What one sweep repaired.
   *
   * @param reclaimed keys whose holder went silent mid-handler
   * @param recovered workers that died before their claim was granted
   * @param released keys whose retry backoff elapsed
   */
  final case class Swept(
    reclaimed: Chunk[MessageKey],
    recovered: Chunk[WorkerId],
    released: Chunk[MessageKey],
  ):

    /** True when nothing needed repairing, which is the normal case and not worth logging. */
    def isEmpty: Boolean = reclaimed.isEmpty && recovered.isEmpty && released.isEmpty
