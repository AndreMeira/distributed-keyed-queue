package homelab.keyedqueue.domain.response.queue


import homelab.keyedqueue.domain.types.ClaimRef
import zio.Chunk

import java.time.Instant


/**
 * The outcome of a heartbeat.
 *
 * @param stale the receipts the caller no longer holds, echoed as it sent them; it must stop working
 *              those. May include receipts this service never issued
 * @param renewedUntil the new deadline for everything else, on the store's clock
 */
final case class HeartbeatResponse(stale: Chunk[String], renewedUntil: Instant)
