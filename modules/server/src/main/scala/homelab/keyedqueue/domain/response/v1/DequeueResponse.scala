package homelab.keyedqueue.domain.response.v1


import homelab.keyedqueue.domain.model.{ Claimed, Message }
import homelab.keyedqueue.domain.types.{ ClaimRef, MessageId }
import zio.Chunk

import java.time.Instant


/**
 * The outcome of a wait: a claim over some of one key's messages, or nothing.
 *
 * The one response that does not mirror its wire message field for field: there the fields are optional,
 * and here the choice is stated outright. Note what is *not* an error — an empty dequeue is an ordinary
 * result of an idle queue.
 *
 * An enum rather than a record of options, because the fields are not independently optional — a claim
 * either happened, in which case there is a receipt, a first message and a lease, or it did not, in which
 * case there is none of them. Making that a choice the caller has to match on is what stops the empty case
 * being reached for through a field that is only sometimes there.
 */
enum DequeueResponse:

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
    head: DequeueResponse.Delivery,
    tail: Chunk[DequeueResponse.Delivery],
    leaseExpiresAt: Instant,
    backlogDepth: Int,
  )


object DequeueResponse:

  /**
   * One message handed to a consumer as part of a claim.
   *
   * Carries no handle of its own: the receipt belongs to the claim, and every message in a batch is settled
   * against it by naming this id. That is the shape of the guarantee — a key is owned, and its messages are
   * what ownership gives access to.
   *
   * Lives here rather than in `domain/model/` because it is not something the queue holds: nothing is
   * stored as a delivery, and no port speaks in them. It exists only as the shape a dequeue hands back,
   * which is what makes it a response type.
   *
   * @param messageId what a settle names this message by
   * @param message the message
   * @param attempt how many times this message has been delivered; 1 on the first
   */
  final case class Delivery(messageId: MessageId, message: Message, attempt: Int)

  /**
   * Say what the store granted in the words the caller is answered in.
   *
   * '''Returns `NonEmpty`, not `DequeueResponse`.''' A [[homelab.keyedqueue.domain.model.Claimed]] carries a
   * `NonEmptyChunk` of messages, so a granted claim can never produce the empty case — and saying so in the
   * return type keeps the caller's `Empty` where it belongs, on the branch where the store returned nothing
   * at all. It is also what lets `head` and `tail` be split here without an `Option`.
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
  def fromClaimed(claimed: Claimed): DequeueResponse.NonEmpty =
    val deliveries = claimed.messages.map: owned =>
      Delivery(owned.id, owned.message, owned.attempt)

    DequeueResponse.NonEmpty(
      claimed.claim.reference,
      deliveries.head,
      Chunk.fromIterable(deliveries.tail),
      claimed.leaseExpiresAt,
      claimed.backlogDepth,
    )
