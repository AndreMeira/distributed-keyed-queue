package homelab.keyedqueue.domain.model.queue


import homelab.keyedqueue.domain.types.MessageId
import zio.{ Duration, NonEmptyChunk }

import java.time.Instant


/**
 * What a granted claim hands over: a batch of one key's messages, and the claim that owns them.
 *
 * One claim, many messages: exclusivity is held on the key, so everything here is under the same token
 * and the same lease. The consumer settles each message as it finishes with it, and the claim ends
 * when nothing is left owed.
 *
 * The messages are in producer order and keep their place while owned, so a nack puts nothing back and a
 * consumer that dies loses only the acknowledgements it had not sent.
 *
 * @param claim which key, in which queue, under which generation
 * @param messages what it may work, oldest first. Never empty: a claim over nothing is not a claim, and
 *                 the store answers with nothing at all rather than with an empty one
 * @param leaseExpiresAt when the claim lapses unless renewed, on the store's clock
 * @param leaseTtl how long the lease runs, which a consumer times its heartbeats by
 * @param backlogDepth how many more were queued for this key, behind the batch
 */
final case class Grant(
  claim: Claim,
  messages: NonEmptyChunk[Grant.Owned],
  leaseExpiresAt: Instant,
  leaseTtl: Duration,
  backlogDepth: Int,
)


object Grant:

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
