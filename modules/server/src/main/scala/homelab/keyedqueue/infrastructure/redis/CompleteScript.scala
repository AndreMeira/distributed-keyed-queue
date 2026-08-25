package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.ClaimRef
import homelab.keyedqueue.domain.types.*
import io.lettuce.core.ScriptOutputType
import zio.*


/**
 * Settle the in-flight message and decide the key's next state — `lua/complete.lua`.
 *
 * The script advances the key's fence as well as applying the verdict, so a replayed settle finds its token
 * spent and reports stale rather than acting twice.
 *
 * '''The namespace is derived, not passed.''' A [[ClaimRef]] already names its queue, and a caller free to
 * supply a namespace alongside it is a caller free to supply the wrong one — which would settle a claim
 * against another queue's keys.
 *
 * @param sha the digest this script was loaded under
 * @param claim the claim being settled, and the token that authorises it
 * @param verdict what became of the message
 * @param retryAfter how long to hold a failed message back; ignored when the verdict is `Done`
 */
final case class CompleteScript(sha: String, claim: ClaimRef, verdict: Verdict, retryAfter: Duration) extends LuaScript[Boolean]:

  private val ns: Namespace = Namespace(claim.queue)

  /**
   * Integer, because the script answers whether it applied and nothing more.
   *
   * @return `INTEGER`
   */
  override def output: ScriptOutputType = ScriptOutputType.INTEGER

  /**
   * Everything a settle can move: the key's state and lease, its fence, both of its message lists, the
   * ready list a `Done` may put it back on, its attempt count, and the backoff a `Failed` may park it in.
   *
   * @return `state`, `claimed`, `fence`, `msgs`, `inflight`, `ready`, `attempts`, `delayed`, in the order
   *         `lua/complete.lua` reads them
   */
  override def keys: Array[String] =
    Array(ns.state, ns.claimed, ns.fence, ns.msgs(claim.key), ns.inflight(claim.key), ns.ready, ns.attempts, ns.delayed)

  /**
   * The key, the token that authorises this transition, the verdict as the script spells it, and the
   * backoff — which the script ignores unless the verdict is `failed`.
   *
   * @return `key`, `token`, `outcome`, `retryAfter`, in the order `lua/complete.lua` reads them
   */
  override def args: Array[Array[Byte]] =
    Array(
      LuaScript.utf8(claim.key),
      LuaScript.utf8(claim.token.toString),
      LuaScript.utf8(if verdict == Verdict.Done then "done" else "failed"),
      LuaScript.utf8(retryAfter.toMillis.toString),
    )

  /**
   * Read whether the settle applied.
   *
   * `0` is not a failure: it means the token was already spent, so the claim had been revoked or settled
   * before this call arrived. The caller is told, and decides.
   *
   * @param value the raw reply
   * @return whether it applied; aborts with `MalformedReply` when the reply is not an integer
   */
  override def read(value: Any): IO[QueueError, Boolean] =
    ZIO.fromEither(LuaScript.number("complete")(value).map(_ == 1L))
