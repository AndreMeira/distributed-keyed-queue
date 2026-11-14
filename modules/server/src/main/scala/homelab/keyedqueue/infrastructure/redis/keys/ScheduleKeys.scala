package homelab.keyedqueue.infrastructure.redis.keys


/**
 * The keys one partition's scheduled messages wait in, shared by every queue in the partition.
 *
 * One sorted set and one hash per partition rather than per queue: a pass of the mover is one script per
 * partition whatever the number of queues, and no instance needs a list of queues to know what to look at
 * (`docs/research/tick-driven-mover.md`). A member names the queue, the key and the message id, built by the
 * scripts themselves as `<byte length of queue>:<queue><byte length of key>:<key><message id>`, so no
 * character of a name is ever mistaken for a separator.
 *
 * Every queue key the move script appends to is built at runtime from [[queues]]; that is legal on a cluster
 * because the partition's tag puts all of them in one slot.
 *
 * @param tag the hash tag of the partition these keys belong to
 */
final case class ScheduleKeys(tag: KeyLayout.Tag):

  /** member -> when the message falls due, unix millis on the store's clock. */
  val scheduled: RedisKey = RedisKey(s"$tag:${KeyLayout.segment}:scheduled")

  /** member -> the message as stored, until it is moved. */
  val payloads: RedisKey = RedisKey(s"$tag:${KeyLayout.segment}:scheduled:payloads")

  /** What every queue key in the partition starts with; a queue's own prefix is this followed by its name. */
  val queues: String = s"$tag:${KeyLayout.segment}:q:"

  /** The partition's wake stream, which a move announces on once per queue it made a key claimable in. */
  val wake: RedisKey = KeyLayout.wakeStreamKey(tag)

  /** The partition's held-back keys, which a move reads to tell whether a key is waiting out a backoff. */
  val delayed: RedisKey = KeyLayout.delayedKey(tag)
