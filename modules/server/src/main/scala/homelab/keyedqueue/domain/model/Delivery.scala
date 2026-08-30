package homelab.keyedqueue.domain.model

import homelab.keyedqueue.domain.types.MessageId

/**
 * One message handed to a consumer as part of a claim.
 *
 * Carries no handle of its own: the receipt belongs to the claim, and every message in a batch is settled
 * against it by naming this id. That is the shape of the guarantee — a key is owned, and its messages are
 * what ownership gives access to.
 *
 * @param messageId what a settle names this message by
 * @param message the message
 * @param attempt how many times this message has been delivered; 1 on the first
 */
final case class Delivery(messageId: MessageId, message: Message, attempt: Int)
