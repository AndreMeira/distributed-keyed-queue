package homelab.keyedqueue.infrastructure.redis.keys


/**
 * The keys one partition's held-back keys wait in, shared by every queue in the partition.
 *
 * A nack's wait holds back its key in [[delayed]], one sorted set per partition: a pass of the backoff mover is one
 * script per partition whatever the number of queues, and no instance needs a list of queues to know what to look
 * at. A member names the queue and the key, `<byte length of queue>:<queue><key>`, so no character of a name is
 * mistaken for a separator.
 *
 * @param tag the hash tag of the partition these keys belong to
 */
final case class BackoffKeys(tag: KeyLayout.Tag):

  /** member -> when the key may be worked again, unix millis on the store's clock. */
  val delayed: RedisKey = KeyLayout.delayedKey(tag)

  /** What every queue key in the partition starts with; a queue's own prefix is this followed by its name. */
  val queues: String = s"$tag:${KeyLayout.segment}:q:"

  /** The partition's wake stream, which a release announces on once per queue it made a key claimable in. */
  val wake: RedisKey = KeyLayout.wakeStreamKey(tag)
