package homelab.keyedqueue.domain.model


import homelab.keyedqueue.domain.types.MessageId
import zio.Chunk

import java.time.Instant


/**
 * A batch of one key's messages, and the claim that owns them.
 *
 * '''One claim, many messages.''' Exclusivity is granted on the key, so everything here is held by the same
 * token and released by the same lease. The consumer settles each message as it finishes with it, and the
 * claim ends when nothing is left owed.
 *
 * The messages are in producer order, and they stay in the queue's own list while they are owned — a nack
 * therefore puts nothing back, and a consumer that dies loses only the acknowledgements it had not sent.
 *
 * @param claim which key, in which queue, under which generation
 * @param messages what it may work, oldest first
 * @param leaseExpiresAt when the claim lapses unless renewed, on the store's clock
 * @param backlogDepth how many more were queued for this key, behind the batch
 */
final case class Claimed(
  claim: Claim,
  messages: Chunk[Claimed.Owned],
  leaseExpiresAt: Instant,
  backlogDepth: Int,
)


object Claimed:

  /**
   * One message of a batch, and how often it has been handed out.
   *
   * The count is per message rather than per key: with several owned at once, "how many times has this been
   * delivered" stops being a question about the key. A nacked message keeps its count and climbs on
   * redelivery, which is what makes a poison message visible.
   *
   * @param id what a settle names it by
   * @param message the message itself
   * @param attempt how many times it has been delivered; 1 on the first
   */
  final case class Owned(id: MessageId, message: Message, attempt: Int)
