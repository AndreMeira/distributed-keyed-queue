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

  /** key -> how many times the current head message has been delivered. */
  val attempts: String = s"$prefix:attempts"

  /** worker -> liveness deadline. What makes a claiming list recoverable. */
  val workers: String = s"$prefix:workers"

  /** key -> when a failed message may be retried. */
  val delayed: String = s"$prefix:delayed"

  /**
   * That key's messages, oldest first.
   *
   * @param key the key
   * @return the list name
   */
  def msgs(key: MessageKey): String = s"$prefix:msgs:$key"

  /**
   * The one message a key is currently working.
   *
   * @param key the key
   * @return the list name
   */
  def inflight(key: MessageKey): String = s"$prefix:inflight:$key"

  /**
   * One worker's in-transition keys: what a `BLMOVE` lands in, and what its death leaves behind.
   *
   * @param worker the worker
   * @return the list name
   */
  def claiming(worker: WorkerId): String = s"$prefix:claiming:$worker"
