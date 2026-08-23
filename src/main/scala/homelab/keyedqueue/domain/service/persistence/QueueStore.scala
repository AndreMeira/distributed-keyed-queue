package homelab.keyedqueue.domain.service.persistence


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ ClaimRef, Claimed }
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
 * '''Payloads are opaque.''' The store never parses a message; the layer above encodes an `Envelope` into
 * bytes and decodes it on the way out. Only the key is structural, because ordering is defined by it.
 */
trait QueueStore:

  /**
   * Accept a message for a key, and make the key claimable if nothing is working it.
   *
   * @param queue the queue to append to
   * @param key the key that orders this message
   * @param payload the message as bytes
   * @return the key's queue depth after the append; aborts with `QueueError` if the store fails
   */
  def enqueue(queue: QueueName, key: MessageKey, payload: Chunk[Byte]): IO[QueueError, Long]

  /**
   * Wait up to `timeout` for a key to become claimable, then take its oldest message.
   *
   * Blocks the calling fiber, and with it the connection this store instance owns, which is why a claimer is
   * a scarce resource rather than something to fork per request.
   *
   * @param queue the queue to claim from
   * @param timeout how long to wait for work
   * @return the claim, or `None` when nothing became claimable in time; aborts with `QueueError` if the
   *         store fails
   */
  def claim(queue: QueueName, timeout: Duration): IO[QueueError, Option[Claimed]]

  /**
   * Report what happened to a claimed message.
   *
   * @param claim the claim being settled
   * @param verdict what the consumer decided
   * @param retryAfter how long to hold the key back before retrying; ignored for `Verdict.Done`
   * @return true when applied, false when the claim had already been revoked; aborts with `QueueError` if
   *         the store fails
   */
  def settle(claim: ClaimRef, verdict: Verdict, retryAfter: Duration): IO[QueueError, Boolean]

  /**
   * Push the deadline forward on the claims still held, and say which are gone.
   *
   * @param claims everything the caller believes it holds; may span queues
   * @return the new deadline, and the claims that were not renewed because they had been revoked; aborts
   *         with `QueueError` if the store fails
   */
  def renew(claims: Chunk[ClaimRef]): IO[QueueError, (Instant, Chunk[ClaimRef])]

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
