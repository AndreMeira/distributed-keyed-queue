package homelab.keyedqueue.infrastructure.redis.keys

import homelab.keyedqueue.domain.types.LockName


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
 * '''The wake stream is the partition's, not the locks'.''' It is the one [[QueueKeys]] announces on too:
 * an entry says which kind of thing it names, so sharing a stream costs nothing and saves the slot — and
 * the connection blocked on it — that a second one would need.
 *
 * Unlike [[QueueKeys]], the structures are shared by every lock in the partition rather than being one
 * lock's own: `held` and `tokens` carry the name as a member or field. Only `waiters` is a key per lock,
 * and it exists only while someone queues.
 *
 * @param tag the hash tag of the partition these keys belong to
 */
final case class LockKeys(tag: KeyLayout.Tag):

  /** The tag every key in this partition shares. */
  private val prefix: String = s"$tag:${KeyLayout.segment}:l"

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
  val wake: RedisKey = KeyLayout.wakeStreamKey(tag)

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
