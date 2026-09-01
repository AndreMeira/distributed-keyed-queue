package homelab.keyedqueue.domain.response.v1


import homelab.keyedqueue.domain.model.{ Claimed, Message }
import homelab.keyedqueue.domain.types.*
import zio.Chunk

import java.time.Instant


/**
 * What the queue answers.
 *
 * As with the requests, these mirror their wire messages closely enough for the transformer to carry no
 * decisions — [[Dequeue]] being the exception, where the wire's optional fields become a choice the domain
 * states outright. Note what is *not* an error here: an empty dequeue and a `Stale` outcome are ordinary
 * results of an at-least-once queue, carried where a caller has to look at them.
 *
 * Nested rather than four top-level types, so the umbrella is what carries that shared note and the names
 * do not have to repeat the word: `QueueResponse.Dequeue`, not `DequeueResponse`.
 */
object QueueResponse:

  /**
   * A message was accepted.
   *
   * @param keyDepth how many messages its key now has queued — a metric, not a decision
   */
  final case class Enqueue(keyDepth: Long)

  /**
   * The outcome of a wait: a claim over some of one key's messages, or nothing.
   *
   * An enum rather than a record of options, because the fields are not independently optional — a claim
   * either happened, in which case there is a receipt, a first message and a lease, or it did not, in which
   * case there is none of them. Making that a choice the caller has to match on is what stops the empty
   * case being reached for through a field that is only sometimes there.
   */
  enum Dequeue:

    /** The wait elapsed with nothing to hand over. Not an error: an idle queue answers this. */
    case Empty

    /**
     * A claim was granted over one key's messages.
     *
     * @param receipt what every settle for these messages names
     * @param head the message to work; present because a claim was granted, so no caller has to ask whether
     *             the batch is empty
     * @param tail the rest of the batch, in producer order after `head`
     * @param leaseExpiresAt when the whole claim lapses unless renewed
     * @param backlogDepth how many more were queued for this key behind the batch
     */
    case NonEmpty(
      receipt: ClaimRef,
      head: Delivery,
      tail: Chunk[Delivery],
      leaseExpiresAt: Instant,
      backlogDepth: Int,
    )

  object Dequeue:

    /**
     * Say what the store granted in the words the caller is answered in.
     *
     * '''Returns `NonEmpty`, not `Dequeue`.''' A [[homelab.keyedqueue.domain.model.Claimed]] carries a
     * `NonEmptyChunk` of messages, so a granted claim can never produce the empty case — and saying so in
     * the return type keeps the caller's `Empty` where it belongs, on the branch where the store returned
     * nothing at all. It is also what lets `head` and `tail` be split here without an `Option`.
     *
     * The mapping itself carries no decision: `Claimed.Owned` and [[Delivery]] hold the same three fields,
     * with `id` becoming `messageId` because that is what a settle names it by. They stay separate types
     * because one is what a port hands back and the other is what a response carries.
     *
     * Lives on this companion rather than in the use case so the shape and the way to build it travel
     * together — a field added to `NonEmpty` is a compile error here, not somewhere else.
     *
     * @param claimed the batch the store granted
     * @return it as a response, its messages in the order they were handed over
     */
    def fromClaimed(claimed: Claimed): Dequeue.NonEmpty =
      val deliveries = claimed.messages.map: owned =>
        QueueResponse.Delivery(owned.id, owned.message, owned.attempt)

      QueueResponse.Dequeue.NonEmpty(
        claimed.claim.reference,
        deliveries.head,
        Chunk.fromIterable(deliveries.tail),
        claimed.leaseExpiresAt,
        claimed.backlogDepth,
      )

  /**
   * One message handed to a consumer as part of a claim.
   *
   * Carries no handle of its own: the receipt belongs to the claim, and every message in a batch is settled
   * against it by naming this id. That is the shape of the guarantee — a key is owned, and its messages are
   * what ownership gives access to.
   *
   * Lives here rather than in `domain/model/` because it is not something the queue holds: nothing is
   * stored as a delivery, and no port speaks in them. It exists only as the shape [[Dequeue]] hands back,
   * which is what makes it a response type.
   *
   * @param messageId what a settle names this message by
   * @param message the message
   * @param attempt how many times this message has been delivered; 1 on the first
   */
  final case class Delivery(messageId: MessageId, message: Message, attempt: Int)

  /**
   * The outcome of a settle.
   *
   * @param applied whether it landed, or the claim had already been revoked
   */
  final case class Settle(applied: Applied)

  /**
   * The outcome of a heartbeat.
   *
   * @param stale the receipts the caller no longer holds, echoed as it sent them; it must stop working
   *              those. Strings rather than [[ClaimRef]]s because the set includes receipts this service
   *              never issued — calling those evidence of a claim would be a lie, and a caller matches
   *              them against what it sent either way
   * @param renewedUntil the new deadline for everything else, on the store's clock
   */
  final case class Heartbeat(stale: Chunk[String], renewedUntil: Instant)
