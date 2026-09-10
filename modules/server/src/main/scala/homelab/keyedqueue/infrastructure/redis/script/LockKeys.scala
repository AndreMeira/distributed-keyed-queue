package homelab.keyedqueue.infrastructure.redis.script


/**
 * The two keys every lock script touches: held leases, and fence generations.
 *
 * Static, unlike the queue's per-key [[homelab.keyedqueue.infrastructure.redis.Namespace]]: all locks share
 * one `held` zset and one `fence` hash, with the lock's name as a member or field. Both carry one hash tag
 * so a script may touch both, and so every lock lands in one cluster slot.
 */
object LockKeys:

  /** The hash tag every lock key shares. */
  private val tag: String = "{dkq:locks}"

  /** `held`, `fence`, in the order every lock script reads them. */
  val all: Array[String] = Array(s"$tag:held", s"$tag:fence")
