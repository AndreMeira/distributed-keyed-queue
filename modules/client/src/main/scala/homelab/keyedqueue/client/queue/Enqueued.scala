package homelab.keyedqueue.client.queue

/**
 * What accepting a message answered.
 *
 * @param keyDepth how many messages the key now has queued, at the moment it was accepted; a measure to
 *                 report rather than one to decide on, since producers may append at any time
 */
final case class Enqueued(keyDepth: Long)
