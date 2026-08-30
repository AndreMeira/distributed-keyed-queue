package homelab.keyedqueue.domain.model

import java.time.Instant

/**
 * A message handed to a consumer, and everything it needs to keep it or give it back.
 *
 * @param claim which key, in which queue, under which generation
 * @param message the message itself, read back from wherever the store put it
 * @param attempt how many times this message has been delivered; 1 on the first
 * @param leaseExpiresAt when the claim lapses unless renewed, on the store's clock
 * @param backlogDepth how many messages were still queued for this key when this one was claimed
 */
final case class Claimed(claim: Claim, message: Message, attempt: Int, leaseExpiresAt: Instant, backlogDepth: Int)
