package homelab.keyedqueue.infrastructure.redis.script


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.Claim
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.Namespace
import io.lettuce.core.ScriptOutputType
import zio.*


/**
 * Settle the in-flight message and decide the key's next state — `lua/complete.lua`.
 *
 * The script advances the key's fence as well as applying the verdict, so a replayed settle finds its token
 * spent and reports stale rather than acting twice.
 *
 * '''The namespace is derived, not passed.''' A [[Claim]] already names its queue, and a caller free to
 * supply a namespace alongside it is a caller free to supply the wrong one — which would settle a claim
 * against another queue's keys.
 */
object CompleteScript:

  /**
   * Integer, because the script answers whether it applied and nothing more.
   *
   * @return `INTEGER`
   */
  def output: ScriptOutputType = ScriptOutputType.INTEGER

  /**
   * Everything a settle can move: the key's state and lease, its fence, both of its message lists, the
   * ready list a `Done` may put it back on, its attempt count, and the backoff a `Failed` may park it in.
   *
   * @param claim the claim being settled, which names the queue these keys belong to
   * @return `state`, `claimed`, `fence`, `msgs`, `inflight`, `ready`, `attempts`, `delayed`, in the order
   *         `lua/complete.lua` reads them
   */
  def keys(claim: Claim): Array[String] =
    val ns = Namespace(claim.queue)
    Array(
      ns.state,
      ns.claimed,
      ns.fence,
      ns.msgs(claim.key),
      ns.inflight(claim.key),
      ns.ready,
      ns.attempts,
      ns.delayed,
    )

  /**
   * The key, the token that authorises this transition, the verdict as the script spells it, and the
   * backoff — which the script ignores unless the verdict is `failed`.
   *
   * @param claim the claim being settled, and the token that authorises it
   * @param verdict what became of the message
   * @param retryAfter how long to hold a failed message back; ignored when the verdict is `Done`
   * @return `key`, `token`, `outcome`, `retryAfter`, in the order `lua/complete.lua` reads them
   */
  def args(claim: Claim, verdict: Verdict, retryAfter: Duration): Array[Array[Byte]] =
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
   * @return whether it applied, or `MalformedReply` when the reply is not an integer
   */
  def read(value: Any): Either[QueueError, Boolean] =
    LuaScript.Decode.long.map(_ == 1L).decode("complete", value)
