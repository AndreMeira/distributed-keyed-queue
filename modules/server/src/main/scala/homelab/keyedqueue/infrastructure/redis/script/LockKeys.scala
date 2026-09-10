package homelab.keyedqueue.infrastructure.redis.script


/**
 * The keys every lock script touches: held leases, fence generations, and the wake stream.
 *
 * Static, unlike the queue's per-key [[homelab.keyedqueue.infrastructure.redis.Namespace]]: all locks share
 * one `held` zset and one `fence` hash, with the lock's name as a member or field, and one `wake` stream a
 * release appends to. All carry one hash tag so a script may touch them together, and so every lock lands
 * in one cluster slot. (Bucketing the tag by lock name — as the queue does — is deferred; this is the
 * single-slot form.)
 */
object LockKeys:

  /** The hash tag every lock key shares. */
  private val tag: String = "{dkq:locks}"

  /** Held leases, `name -> deadline`. */
  val held: String = s"$tag:held"

  /** Fence generations, `name -> counter`. */
  val fence: String = s"$tag:fence"

  /** The wake stream a release appends to, read by the shared listener. */
  val wake: String = s"$tag:wake"

  /** `held`, `fence` — what acquire and refresh read. */
  val core: Array[String] = Array(held, fence)

  /** `held`, `fence`, `wake` — release also appends a wake. */
  val withWake: Array[String] = Array(held, fence, wake)
