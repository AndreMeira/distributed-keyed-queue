package homelab.keyedqueue.domain.request.v1

import zio.Chunk

/**
 * Renew everything a consumer still holds.
 *
 * Mirrors its wire message field for field, so the transformer between them carries no decisions. The
 * receipts are the strings a caller sent rather than `ClaimRef`s, for the reason given on [[SettleRequest]]:
 * a handle nobody has checked is not evidence of a claim.
 *
 * @param receipts the handles it believes it holds, as they arrived; empty is legal and means "still
 *                 here, holding nothing"
 */
final case class HeartbeatRequest(receipts: Chunk[String])
