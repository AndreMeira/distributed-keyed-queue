package homelab.keyedqueue.domain.response.v1

/**
 * A message was accepted.
 *
 * @param keyDepth how many messages its key now has queued — a metric, not a decision
 */
final case class EnqueueResponse(keyDepth: Long)
