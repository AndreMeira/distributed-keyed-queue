package homelab.keyedqueue.client.queue.model


import zio.{ Duration, NonEmptyChunk }

import java.time.Instant


/**
 * A key's messages, and what owns them until they are settled.
 *
 * One claim over many messages: exclusivity is held on the key, so everything here is under the same
 * receipt and the same lease. A consumer settles each message as it finishes with it, and the claim ends
 * once nothing is left owed.
 *
 * @param receipt what every settle and every heartbeat for these messages names
 * @param messages what to work, oldest first; never empty, because a claim over nothing is not a claim
 * @param leaseExpiresAt when the whole claim lapses unless renewed, on the service's clock
 * @param leaseTtl how long the lease runs, which is what a heartbeat cadence is measured in
 * @param backlogDepth how many more were queued for this key behind these, at the moment it was granted
 */
final case class Claim(
  receipt: Receipt,
  messages: NonEmptyChunk[Message.Incoming],
  leaseExpiresAt: Instant,
  leaseTtl: Duration,
  backlogDepth: Int,
)
