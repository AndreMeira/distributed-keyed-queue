package homelab.keyedqueue.infrastructure.redis.keys


import homelab.keyedqueue.domain.types.LockName
import zio.{ Chunk, NonEmptyChunk }


/**
 * The keys one partition's lock scripts touch: held leases, live holders' tokens, the fence counter, the
 * waiting index and per-lock waiter lists, and the wake stream.
 *
 * '''A lock's partition follows its name''', as a queue's follows the queue's, so every operation on one
 * lock reaches the same keys without being told which partition it is in. What the partition buys is what
 * it buys for the queue: the locks spread across cluster slots instead of every lock in the deployment
 * living on one node — and the wake stream spreads with them, which it must, because a release appends to
 * it in the same script that frees the lock, and a script may only touch one slot.
 *
 * Unlike [[QueueKeys]], the structures are shared by every lock in the partition rather than being one
 * lock's own: `held` and `tokens` carry the name as a member or field. Only `waiters` is a key per lock,
 * and it exists only while someone queues.
 *
 * @param partition which partition these keys belong to
 */
final case class LockKeys(partition: Int):

  /** The tag every key in this partition shares. */
  private val prefix: String = s"${LockKeys.tag(partition)}:${KeyLayout.segment}"

  /** Held leases, `name -> deadline`. */
  val held: RedisKey = RedisKey(s"$prefix:held")

  /** Live holders' fence tokens, `name -> token`; an entry dies with its hold. */
  val tokens: RedisKey = RedisKey(s"$prefix:tokens")

  /**
   * The fence counter for this partition; the only key that outlives a hold.
   *
   * One counter per partition rather than one for the deployment, which is safe because a fence need only
   * increase within a single lock, and a lock never changes partition.
   */
  val fence: RedisKey = RedisKey(s"$prefix:fence")

  /** Which locks have waiters, `name -> the latest ticket deadline` — what the trim prunes dead lists by. */
  val waiting: RedisKey = RedisKey(s"$prefix:waiting")

  /** What a lock's waiters-list key starts with; the name completes it. */
  val waitersPrefix: String = s"$prefix:waiters:"

  /** The wake stream a release appends to, read by the shared listener. */
  val wake: RedisKey = LockKeys.wake(partition)

  /**
   * One lock's waiters list: its tickets, in arrival order. Exists only while someone queues.
   *
   * @param name the lock
   * @return the list's key
   */
  def waiters(name: LockName): RedisKey = RedisKey(waitersPrefix + name)

  /**
   * `held`, `tokens`, `fence`, `waiting`, `waiters` — what granting reads and a queued entry writes; the
   * set `acquire`, `grant` and `try` share.
   *
   * @param name the lock
   * @return the keys, in the order the scripts read them
   */
  def granting(name: LockName): Array[String] = Array(held, tokens, fence, waiting, waiters(name))

  /**
   * `waiting`, `waiters` — what withdrawing a ticket touches.
   *
   * @param name the lock
   * @return the keys, in the order `lua/lock/abandon.lua` reads them
   */
  def ticket(name: LockName): Array[String] = Array(waiting, waiters(name))

  /** `held`, `tokens` — a refresh checks the token and moves the lease. */
  val refresh: Array[String] = Array(held, tokens)

  /** `held`, `tokens`, `wake` — a release checks the token, frees both, and appends a wake. */
  val release: Array[String] = Array(held, tokens, wake)

  /** `held`, `tokens`, `wake`, `waiting` — a trim frees abandoned holds and deletes dead waiter lists. */
  val trim: Array[String] = Array(held, tokens, wake, waiting)


object LockKeys:

  /**
   * How many partitions the locks are divided into — fixed in code, not configured, for the reasons
   * [[QueueKeys.partitions]] gives.
   *
   * Its own count and its own tag space rather than the queue's: sharing a tag would put a lock's wake
   * entries on the queue's stream, and a stream feeds exactly one waker — a queue's readiness hands one
   * token to one consumer, a lock's broadcast wakes everyone.
   *
   * '''Part of the schema.''' Changing it moves locks between tags and strands whatever was written under
   * the old count, so bump `KeyLayout.schemaVersion` with it.
   */
  val partitions: Int = 16

  /**
   * The hash tag a partition's keys share.
   *
   * @param partition the partition
   * @return the tag, braces included, so Redis hashes only what is inside them
   */
  def tag(partition: Int): String = s"{l:$partition}"

  /**
   * A partition's wake stream.
   *
   * @param partition the partition
   * @return the stream name
   */
  def wake(partition: Int): RedisKey = RedisKey(s"${tag(partition)}:${KeyLayout.segment}:wake")

  /**
   * Which partition a lock falls in.
   *
   * `String`'s hash is specified by the JVM, so every instance agrees on where a lock lives without being
   * told. `floorMod`, because a negative hash would otherwise produce a negative partition.
   *
   * @param name the lock
   * @return the partition
   */
  def partitionOf(name: LockName): Int = Math.floorMod(name.toString.hashCode, partitions)

  /**
   * The keys of the partition a lock falls in.
   *
   * @param name the lock
   * @return its partition's keys
   */
  def of(name: LockName): LockKeys = LockKeys(partitionOf(name))

  /**
   * Every partition's keys, for the passes that are not about one lock — the trim, which sweeps them all.
   *
   * @return the keys, in partition order
   */
  val all: Chunk[LockKeys] = Chunk.fromIterable(0 until partitions).map(LockKeys(_))

  /**
   * Every wake stream the locks announce on — the fixed set a listener reads.
   *
   * @return the stream names, in partition order
   */
  val wakeStreams: NonEmptyChunk[RedisKey] =
    NonEmptyChunk.fromChunk(Chunk.fromIterable(0 until partitions).map(wake)).getOrElse(NonEmptyChunk(wake(0)))
