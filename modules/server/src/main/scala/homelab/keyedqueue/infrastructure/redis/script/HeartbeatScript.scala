package homelab.keyedqueue.infrastructure.redis.script


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.Claim
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.Connection.Commands
import homelab.keyedqueue.infrastructure.redis.{ Connection, Namespace }
import io.lettuce.core.ScriptOutputType
import zio.*

import java.time.Instant


/**
 * Push the deadline forward on the claims a consumer still holds — `lua/heartbeat.lua`.
 *
 * '''Registration and renewal are the same call.''' A claiming connection with nothing held beats with
 */
final class HeartbeatScript(ref: LuaScript.Sha):

  /** Multi, because a beat answers with a deadline and a list. */
  private val output: ScriptOutputType = ScriptOutputType.MULTI

  /**
   * Push forward the claims named.
   *
   * A consumer is not a worker with a registration of its own: its claims are found by fence token, so
   * there is nothing here to announce and nothing to expire but the leases themselves.
   *
   * @param ns the queue whose claims to renew
   * @param leaseTtl how long the registration, and each renewed claim, survive without another beat
   * @param held the claims to renew, all in that queue
   * @return the new deadline and the claims already revoked; aborts with `QueueError` when the store fails
   *         or the reply cannot be read
   */
  def run(
    ns: Namespace,
    leaseTtl: Duration,
    held: Chunk[Claim],
  ): ZIO[Connection.Commands, QueueError, (Instant, Chunk[Claim])] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.evalsha[Any](ref, output, keys(ns), args(leaseTtl, held)*))
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(read(held)(reply)))

  /**
   * The leases to push forward, and the fences that say whether a claim is still the caller's.
   *
   * @param ns the queue whose claims to renew
   * @return `claimed`, `fence`, in the order `lua/heartbeat.lua` reads them
   */
  private def keys(ns: Namespace): Array[String] = Array(ns.claimed, ns.fence)

  /**
   * The lease length, then the held claims flattened into key-and-token pairs.
   *
   * The token travels with each key because a beat must not renew a claim the caller no longer owns: the
   * script checks it against the fence and reports the key as lost instead.
   *
   * @param leaseTtl how long the registration, and each renewed claim, survive without another beat
   * @param held the claims to renew
   * @return `ttl`, then `key`, `token` repeated, in the order `lua/heartbeat.lua` reads them
   */
  private def args(leaseTtl: Duration, held: Chunk[Claim]): Array[Array[Byte]] =
    val pairs = held.flatMap(claim => Chunk(LuaScript.utf8(claim.key), LuaScript.utf8(claim.token.toString)))
    (Chunk(LuaScript.utf8(leaseTtl.toMillis.toString)) ++ pairs).toArray

  /**
   * Read the new deadline and the claims that were already gone.
   *
   * The script reports the keys it could ''not'' renew, and they are matched back to the claims that were
   * sent: the caller has to stop working those, and a key alone would not say which claim it lost — which is
   * why the claims sent are a parameter here rather than something the reply carries.
   *
   * @param held the claims the beat was sent with
   * @param value the raw reply
   * @return the deadline every renewed claim now carries, and the claims that had already been revoked, or
   *         `MalformedReply` naming the element that could not be read
   */
  private def read(held: Chunk[Claim])(value: Any): Either[QueueError, (Instant, Chunk[Claim])] =
    decoder(held).decode("heartbeat", value)

  /**
   * The shape the script promises: `{renewedUntil, staleKeys}`.
   *
   * The claims are closed over rather than read out of the reply, because the reply names only the keys it
   * could not renew — turning those back into claims is what the caller actually needs, and only the beat
   * that was sent knows which claim each key belonged to.
   *
   * @param held the claims the beat was sent with
   * @return the decoder
   */
  private def decoder(held: Chunk[Claim]): LuaScript.Decode.Of[(Instant, Chunk[Claim])] =
    LuaScript.Decode.sized(2) {
      for
        until <- LuaScript.Decode.long.at(0)
        lost  <- LuaScript.Decode.text.each.at(1)
      yield (Instant.ofEpochMilli(until), held.filter(claim => lost.toSet.contains(claim.key)))
    }


object HeartbeatScript:

  /**
   * Register `lua/heartbeat.lua` and hold the digest it was given.
   *
   * @return the script, ready to run; aborts with `QueueError` if it is missing or the server rejects it
   */
  def make: ZIO[Connection.Commands, QueueError, HeartbeatScript] =
    LuaScript.register("lua/heartbeat.lua").map(HeartbeatScript(_))
