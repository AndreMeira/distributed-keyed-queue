package homelab.keyedqueue.infrastructure.redis.script


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.Claim
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.Connection.Commands
import homelab.keyedqueue.infrastructure.redis.{ Connection, Namespace }
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
 *
 * @param ref the digest this script was loaded under, from [[Scripts]]
 */
final class CompleteScript(ref: LuaScript.Sha):

  /** Integer, because the script answers whether it applied and nothing more. */
  private val output: ScriptOutputType = ScriptOutputType.INTEGER

  /**
   * Settle the in-flight message and decide the key's next state.
   *
   * @param claim the claim being settled, and the token that authorises it
   * @param verdict what became of the message
   * @param retryAfter how long to hold a failed message back; ignored when the verdict is `Done`
   * @param discardAhead how many messages behind this one to drop as superseded; `Done` only
   * @return whether it applied; aborts with `QueueError` when the store fails or the reply cannot be read
   */
  def run(
    claim: Claim,
    verdict: Verdict,
    retryAfter: Duration,
    discardAhead: Int,
  ): ZIO[Connection.Commands, QueueError, Boolean] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(
          redis.evalsha[Any](ref, output, keys(claim), args(claim, verdict, retryAfter, discardAhead)*)
        )
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(read(reply)))

  /**
   * Everything a settle can move: the key's state and lease, its fence, both of its message lists, the
   * ready list a `Done` may put it back on, its attempt count, and the backoff a `Failed` may park it in.
   *
   * @param claim the claim being settled, which names the queue these keys belong to
   * @return `state`, `claimed`, `fence`, `msgs`, `inflight`, `ready`, `attempts`, `delayed`, in the order
   *         `lua/complete.lua` reads them
   */
  private def keys(claim: Claim): Array[String] =
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
   * @param discardAhead how many messages behind this one to drop, counted from the head so a producer's
   *                     concurrent append cannot be caught by it; the script ignores it unless the verdict
   *                     is `done`
   * @return `key`, `token`, `outcome`, `retryAfter`, `discard`, in the order `lua/complete.lua` reads them
   */
  private def args(
    claim: Claim,
    verdict: Verdict,
    retryAfter: Duration,
    discardAhead: Int,
  ): Array[Array[Byte]] =
    Array(
      LuaScript.utf8(claim.key),
      LuaScript.utf8(claim.token.toString),
      LuaScript.utf8(if verdict == Verdict.Done then "done" else "failed"),
      LuaScript.utf8(retryAfter.toMillis.toString),
      LuaScript.utf8(discardAhead.toString),
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
  private def read(value: Any): Either[QueueError, Boolean] =
    LuaScript.Decode.long.map(_ == 1L).decode("complete", value)


object CompleteScript:

  /**
   * Register `lua/complete.lua` and hold the digest it was given.
   *
   * @return the script, ready to run; aborts with `QueueError` if it is missing or the server rejects it
   */
  def make: ZIO[Connection.Commands, QueueError, CompleteScript] =
    LuaScript.register("lua/complete.lua").map(CompleteScript(_))
