package homelab.keyedqueue.domain.response.v1


import homelab.keyedqueue.domain.model.{ Grant, Message }
import homelab.keyedqueue.domain.types.{ ClaimRef, MessageId }
import zio.Chunk

import java.time.Instant


/**
 * The outcome of a wait: a grant over some of one key's messages, or nothing. Empty is an ordinary result
 * of an idle queue, not an error.
 *
 * An enum rather than a record of options, because the fields are not independently optional: a claim
 * either happened — receipt, first message and lease — or it did not, and none of them exist. Matching is
 * what stops the empty case being reached for through a field that is only sometimes there.
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
   * Transform a Grant into a DequeueResponse, which is the shape the API returns it in.
   *
   * @param claimed the batch the store granted
   * @return it as a response, its messages in the order they were handed over
   */
  def fromGrant(grant: Grant): DequeueResponse.NonEmpty =
    val deliveries = grant.messages.map: owned =>
      Delivery(owned.id, owned.message, owned.attempt)

    DequeueResponse.NonEmpty(
      grant.claim.reference,
      deliveries.head,
      Chunk.fromIterable(deliveries.tail),
      grant.leaseExpiresAt,
      grant.backlogDepth,
    )
