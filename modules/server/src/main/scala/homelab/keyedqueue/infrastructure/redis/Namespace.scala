package homelab.keyedqueue.infrastructure.redis

import homelab.keyedqueue.domain.types.*


/**
 * Every Redis key one queue owns, derived from its name.
 *
 * The single `{q:<queue>}` hash tag is load-bearing: a LuaScript script may only touch keys in one cluster slot,
 * and the sweep builds some of its key names at runtime from `prefix`, so they all have to hash together.
 *
 * @param queue the queue these keys belong to
 */
final case class Namespace(queue: QueueName):

  /** The tag every key shares, and what the sweep rebuilds the per-key names from. */
  val prefix: String = s"{q:$queue}"

  /** Keys with work and nobody working them. The only place anything blocks. */
  val ready: String = s"$prefix:ready"

  /** key -> queued | processing. Absence means idle, which keeps the hash small. */
  val state: String = s"$prefix:state"

  /** key -> lease deadline, in unix millis. */
  val claimed: String = s"$prefix:claimed"

  /** key -> claim generation. A token authorises exactly one transition. */
  val fence: String = s"$prefix:fence"

  /** message id -> how many times it has been delivered. Per message, since a claim may own several. */
  val attempts: String = s"$prefix:attempts"

  /** worker -> liveness deadline. What makes a claiming list recoverable. */
  val workers: String = s"$prefix:workers"

  /** key -> when a failed message may be retried. */
  val delayed: String = s"$prefix:delayed"

  /**
   * That key's message ids, in producer order, until they are acknowledged.
   *
   * The list carries order and nothing else — [[payloads]] carries the messages. A claim marks ids as
   * [[owned]] without taking them out of here, which is why a nack has nothing to put back.
   *
   * @param key the key
   * @return the list name
   */
  def msgs(key: MessageKey): String = s"$prefix:msgs:$key"

  /**
   * That key's messages themselves, by id.
   *
   * Separate from [[msgs]] because the two answer different questions: the list carries order, this carries
   * cargo. Keeping them apart is what lets a script address a message by name without reading inside a
   * payload it cannot parse.
   *
   * @param key the key
   * @return the hash name
   */
  def payloads(key: MessageKey): String = s"$prefix:payloads:$key"

  /**
   * The ids this key's live claim owns and has not yet settled.
   *
   * A claim is over when this is empty. Ownership is the only thing it records — the messages themselves
   * stay in [[msgs]] in producer order for as long as they are unacknowledged, so a nack has nothing to put
   * back and a crash has nothing to recover.
   *
   * @param key the key
   * @return the set name
   */
  def owned(key: MessageKey): String = s"$prefix:owned:$key"

  /**
   * One worker's in-transition keys: what a `BLMOVE` lands in, and what its death leaves behind.
   *
   * @param worker the worker
   * @return the list name
   */
  def claiming(worker: WorkerId): String = s"$prefix:claiming:$worker"
