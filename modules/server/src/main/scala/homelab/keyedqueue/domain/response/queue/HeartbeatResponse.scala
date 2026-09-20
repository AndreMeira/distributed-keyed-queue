package homelab.keyedqueue.domain.response.queue


import zio.{ Chunk, Duration }

import java.time.Instant


/**
 * The outcome of a heartbeat.
 *
 * @param stale the receipts the caller no longer holds, echoed as it sent them; it must stop working
 *              those. May include receipts this service never issued
 * @param renewedUntil the new deadline for everything else, on the store's clock
 * @param leaseTtl how long that renewal runs, which a consumer times its heartbeats by
 */
final case class HeartbeatResponse(stale: Chunk[String], renewedUntil: Instant, leaseTtl: Duration)
