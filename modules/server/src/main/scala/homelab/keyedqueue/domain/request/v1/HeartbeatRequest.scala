package homelab.keyedqueue.domain.request.v1

import zio.Chunk

/**
 * A consumer's unchecked ask to renew everything it still holds.
 *
 * The receipts are the strings it sent rather than `ClaimRef`s, for the reason given on [[SettleRequest]]:
 * a handle nobody has checked is not evidence of a claim.
 *
 * @param receipts the handles it believes it holds, as they arrived; empty is legal and means "still
 *                 here, holding nothing"
 */
final case class HeartbeatRequest(receipts: Chunk[String])
