package homelab.keyedqueue.domain.response.queue

/**
 * A message was accepted.
 *
 * @param keyDepth how many messages its key now has queued — a metric, not a decision
 */
final case class EnqueueResponse(keyDepth: Long)
