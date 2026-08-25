package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.ClaimRef
import homelab.keyedqueue.domain.types.*
import io.lettuce.core.ScriptOutputType
import zio.*

import java.time.Instant


/**
 * Write a worker's liveness, and push forward the claims it names — `lua/heartbeat.lua`.
 *
 * '''Registration and renewal are the same call.''' A claimer with nothing held beats with `held` empty
 * purely to stay in its queue's worker set, because a registration that lapses makes the next claim
 * unrecoverable; a consumer beats with the claims it is working. The script cannot tell the two apart, and
 * neither does this.
 *
 * The namespace is a parameter rather than derived from `held`, since an empty `held` names no queue.
 *
 * @param sha the digest this script was loaded under
 * @param ns the queue whose worker set to write to, and whose claims to renew
 * @param worker whose liveness is being written
 * @param leaseTtl how long the registration, and each renewed claim, survive without another beat
 * @param held the claims to renew alongside it, all in that queue; empty for a bare registration
 */
final case class HeartbeatScript(
  sha: String,
  ns: Namespace,
  worker: WorkerId,
  leaseTtl: Duration,
  held: Chunk[ClaimRef],
) extends LuaScript[(Instant, Chunk[ClaimRef])]:

  /**
   * Multi, because a beat answers with a deadline and a list.
   *
   * @return `MULTI`
   */
  override def output: ScriptOutputType = ScriptOutputType.MULTI

  /**
   * The leases to push forward, the fences that say whether a claim is still the caller's, and the worker
   * set this registration lives in.
   *
   * @return `claimed`, `fence`, `workers`, in the order `lua/heartbeat.lua` reads them
   */
  override def keys: Array[String] = Array(ns.claimed, ns.fence, ns.workers)

  /**
   * The lease length and this worker's identity, then the held claims flattened into key-and-token pairs.
   *
   * The token travels with each key because a beat must not renew a claim the caller no longer owns: the
   * script checks it against the fence and reports the key as lost instead.
   *
   * @return `ttl`, `worker`, then `key`, `token` repeated, in the order `lua/heartbeat.lua` reads them
   */
  override def args: Array[Array[Byte]] =
    val pairs = held.flatMap(claim => Chunk(LuaScript.utf8(claim.key), LuaScript.utf8(claim.token.toString)))
    (Chunk(LuaScript.utf8(leaseTtl.toMillis.toString), LuaScript.utf8(worker)) ++ pairs).toArray

  /**
   * Read the new deadline and the claims that were already gone.
   *
   * The script reports the keys it could '''not''' renew, and they are matched back to the claims that were
   * sent: the caller has to stop working those, and a key alone would not say which claim it lost.
   *
   * @param value the raw reply
   * @return the deadline every renewed claim now carries, and the claims that had already been revoked;
   *         aborts with `MalformedReply` when the reply is not a deadline and a list
   */
  override def read(value: Any): IO[QueueError, (Instant, Chunk[ClaimRef])] = value match
    case values: java.util.List[?] if values.size == 2 =>
      val until = values.get(0) match
        case number: java.lang.Long => number.longValue
        case _                      => 0L
      val lost  = LuaScript.strings(values.get(1)).toSet
      ZIO.succeed((Instant.ofEpochMilli(until), held.filter(claim => lost.contains(claim.key))))
    case other                                         =>
      ZIO.fail(QueueError.MalformedReply(s"heartbeat returned ${LuaScript.describe(other)}"))
