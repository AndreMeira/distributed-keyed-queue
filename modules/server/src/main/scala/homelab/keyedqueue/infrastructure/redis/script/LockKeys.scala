package homelab.keyedqueue.infrastructure.redis.script


import homelab.keyedqueue.domain.types.LockName
import homelab.keyedqueue.infrastructure.redis.{ KeyLayout, RedisKey }


/**
 * The keys the lock scripts touch: held leases, live holders' tokens, the fence counter, the waiting index
 * and per-lock waiter lists, and the wake stream.
 *
 * Static but for the waiter lists, unlike the queue's per-key
 * [[homelab.keyedqueue.infrastructure.redis.Namespace]]: locks share one `held` zset, one `tokens` hash,
 * one `fence` counter and one `waiting` index, with the lock's name as a member or field; only `waiters`
 * is a key per lock, and it exists only while someone queues. All carry one hash tag so a script may touch
 * them together, and so every lock lands in one cluster slot. (Partitioning the tag by lock name — as the
 * queue does — is deferred; this is the single-slot form.)
 */
object LockKeys:

  /** The hash tag every lock key shares. */
  private val tag: String = "{dkq:locks}"

  /** Held leases, `name -> deadline`. */
  val held: RedisKey = RedisKey(s"$tag:${KeyLayout.segment}:held")

  /** Live holders' fence tokens, `name -> token`; an entry dies with its hold. */
  val tokens: RedisKey = RedisKey(s"$tag:${KeyLayout.segment}:tokens")

  /** The fence counter, one for every lock; the only key that outlives a hold. */
  val fence: RedisKey = RedisKey(s"$tag:${KeyLayout.segment}:fence")

  /** Which locks have waiters, `name -> the latest ticket deadline` — what the trim prunes dead lists by. */
  val waiting: RedisKey = RedisKey(s"$tag:${KeyLayout.segment}:waiting")

  /** What a lock's waiters-list key starts with; the name completes it. */
  val waitersPrefix: String = s"$tag:${KeyLayout.segment}:waiters:"

  /** The wake stream a release appends to, read by the shared listener. */
  val wake: RedisKey = RedisKey(s"$tag:${KeyLayout.segment}:wake")

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
