package homelab.keyedqueue.client.queue


/**
 * What reporting an outcome answered.
 */
enum Settled:

  /** The outcomes were recorded, and any message they completed is done with. */
  case Applied

  /** The claim had already been revoked: these outcomes changed nothing, and the work may be redelivered. */
  case Stale
