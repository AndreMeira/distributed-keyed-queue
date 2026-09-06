package homelab.keyedqueue.domain.request.v1

import zio.Chunk

/**
 * A consumer's ask to renew everything it still holds.
 *
 * @param receipts the handles it believes it holds, as they arrived; empty is legal and means "still
 *                 here, holding nothing"
 */
final case class HeartbeatRequest(receipts: Chunk[String])
