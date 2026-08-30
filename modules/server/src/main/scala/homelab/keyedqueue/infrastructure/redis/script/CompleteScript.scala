package homelab.keyedqueue.infrastructure.redis.script


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.Claim
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.{ Connection, Namespace }
import io.lettuce.core.ScriptOutputType
import zio.*


/**
 * Settle some of what a claim owns, and release its key when nothing is left owed — `lua/complete.lua`.
 *
 * '''A claim may be settled piece by piece.''' The token is checked on every call and advanced only when
 * the claim ends, so it stays good across several settles; what stops one applying twice is that settling
 * removes the id from the owned set, and removing it again finds nothing.
 *
 * '''The namespace is derived, not passed.''' A [[Claim]] already names its queue, and a caller free to
 * supply a namespace alongside it is a caller free to supply the wrong one — which would settle against
 * another queue's keys.
 *
 * @param ref the digest this script was loaded under, from [[Scripts]]
 */
final class CompleteScript(ref: LuaScript.Sha):

  /** Integer, because the script answers whether it applied and nothing more. */
  private val output: ScriptOutputType = ScriptOutputType.INTEGER

  /**
   * Report what became of some of the claim's messages.
   *
   * @param claim the claim being settled against, and the token that authorises it
   * @param outcomes what became of each message named, by id
   * @param retryAfter how long the key should wait before anyone works it again, asked for by a nack;
   *                   several nacks in one claim leave the longest wait standing
   * @return whether it applied; aborts with `QueueError` when the store fails or the reply cannot be read
   */
  def run(
    claim: Claim,
    outcomes: Chunk[(MessageId, Verdict)],
    retryAfter: Duration,
  ): ZIO[Connection.Commands, QueueError, Boolean] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.evalsha[Any](ref, output, keys(claim), args(claim, outcomes, retryAfter)*))
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(read(reply)))

  /**
   * Everything a settle can move: the key's state and lease, its fence, its messages and their payloads,
   * what the claim still owns, the delivery counts, the ready list it may return to, and the backoff a nack
   * may park it in.
   *
   * @param claim the claim being settled against, which names the queue these keys belong to
   * @return `state`, `claimed`, `fence`, `msgs`, `payloads`, `owned`, `attempts`, `ready`, `delayed`, in the
   *         order `lua/complete.lua` reads them
   */
  private def keys(claim: Claim): Array[String] =
    val ns = Namespace(claim.queue)
    Array(
      ns.state,
      ns.claimed,
      ns.fence,
      ns.msgs(claim.key),
      ns.payloads(claim.key),
      ns.owned(claim.key),
      ns.attempts,
      ns.ready,
      ns.delayed,
    )

  /**
   * The key, the token that authorises this settle, the backoff, then each message and what became of it.
   *
   * Flattened into pairs rather than sent as a structure, because a script's arguments are a flat array —
   * the script reads them two at a time from position four.
   *
   * @param claim the claim being settled against, and the token that authorises it
   * @param outcomes what became of each message named, by id
   * @param retryAfter how long the key should wait, asked for by a nack
   * @return `key`, `token`, `retryAfter`, then `id`, `verdict` repeated, in the order `lua/complete.lua`
   *         reads them
   */
  private def args(
    claim: Claim,
    outcomes: Chunk[(MessageId, Verdict)],
    retryAfter: Duration,
  ): Array[Array[Byte]] =
    val settle = Chunk(
      LuaScript.utf8(claim.key),
      LuaScript.utf8(claim.token.toString),
      LuaScript.utf8(retryAfter.toMillis.toString),
    )
    val each   = outcomes.flatMap: (id, verdict) =>
      Chunk(LuaScript.utf8(id), LuaScript.utf8(if verdict == Verdict.Done then "ack" else "nack"))
    (settle ++ each).toArray

  /**
   * Read whether the settle applied.
   *
   * `0` is not a failure: it means the token was spent, so the claim had been revoked before this call
   * arrived. The caller is told, and decides.
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
