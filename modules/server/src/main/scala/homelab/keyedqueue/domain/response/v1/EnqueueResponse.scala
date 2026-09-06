package homelab.keyedqueue.domain.response.v1

/**
 * A message was accepted.
 *
 * Mirrors its wire message closely enough for the transformer between them to carry no decisions.
 *
 * @param keyDepth how many messages its key now has queued — a metric, not a decision
 */
final case class EnqueueResponse(keyDepth: Long)
