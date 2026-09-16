package homelab.keyedqueue.infrastructure.redis.keys

import homelab.keyedqueue.domain.types.*


/**
 * Every Redis key one queue owns, derived from its name and the partition it falls in.
 *
 * The hash tag is the partition's, and the schema version follows it — `{p:3}:v3:q:jobs:ready`. So every
 * key a script touches hashes into one slot, including the wake stream the partition shares between its
 * queues, and a schema's leftovers stay findable by pattern.
 *
 * See `docs/architecture/redis-cluster.md` and `docs/research/schema-versioned-keys.md`.
 *
 * @param tag the hash tag of the partition these keys fall in
 * @param queue the queue these keys belong to
 */
final case class QueueKeys(tag: KeyLayout.Tag, queue: QueueName):

  /** The tag every key shares, and what the scripts rebuild the per-key names from. */
  val prefix: String = s"$tag:${KeyLayout.segment}:q:$queue"

  /**
   * Keys with work and nobody working them, scored by when each became claimable.
   */
  val ready: RedisKey = RedisKey(s"$prefix:ready")

  /**
   * The counter that scores [[ready]]: one number per key that becomes claimable, ever increasing.
   *
   * Arrival order, not a clock: the counter has no ties by construction, and every writer agrees on what
   * "older" means without agreeing on a clock. See `docs/architecture/redis-data-structures.md`.
   */
  val sequence: RedisKey = RedisKey(s"$prefix:seq")

  /** key -> lease deadline, in unix millis. */
  val claimed: RedisKey = RedisKey(s"$prefix:claimed")

  /** key -> claim generation. A token authorises exactly one transition. */
  val fence: RedisKey = RedisKey(s"$prefix:fence")

  /** message id -> how many times it has been delivered. Per message, since a claim may own several. */
  val attempts: RedisKey = RedisKey(s"$prefix:attempts")

  /**
   * The stream this queue announces on: one entry per key made claimable, appended by the same script
   * that made it so, and shared with every other queue in the partition.
   */
  val wake: RedisKey = KeyLayout.wakeStreamKey(tag)

  /** key -> when a failed message may be retried. */
  val delayed: RedisKey = RedisKey(s"$prefix:delayed")

  /**
   * That key's message ids, in producer order, until they are acknowledged.
   *
   * The list carries order and nothing else — [[payloads]] carries the messages. A claim marks ids as
   * [[owned]] without taking them out of here, which is why a nack has nothing to put back.
   *
   * @param key the key
   * @return the list name
   */
  def msgs(key: MessageKey): RedisKey = RedisKey(s"$prefix:msgs:$key")

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
  def payloads(key: MessageKey): RedisKey = RedisKey(s"$prefix:payloads:$key")

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
  def owned(key: MessageKey): RedisKey = RedisKey(s"$prefix:owned:$key")
