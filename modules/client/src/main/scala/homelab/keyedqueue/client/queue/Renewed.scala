package homelab.keyedqueue.client.queue


import zio.{ Chunk, Duration }

import java.time.Instant


/**
 * What a heartbeat answered, for every claim it named at once.
 *
 * @param stale the receipts this caller no longer holds; it must stop working those, and their messages
 *              may already be with somebody else
 * @param renewedUntil the new deadline for everything else, on the service's clock
 * @param leaseTtl how long that renewal runs, which is what the next beat is timed by
 */
final case class Renewed(stale: Chunk[Receipt], renewedUntil: Instant, leaseTtl: Duration)
