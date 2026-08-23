package homelab.keyedqueue.domain.model


import zio.Chunk

import java.time.Instant


/**
 * A message handed to a consumer, and everything it needs to keep it or give it back.
 *
 * @param claim which key, in which queue, under which generation
 * @param payload the message as stored: opaque to the store, an `Envelope` to the layer above
 * @param attempt how many times this message has been delivered; 1 on the first
 * @param leaseExpiresAt when the claim lapses unless renewed, on the store's clock
 */
final case class Claimed(claim: ClaimRef, payload: Chunk[Byte], attempt: Int, leaseExpiresAt: Instant)
