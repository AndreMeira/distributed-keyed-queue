package homelab.keyedqueue.infrastructure.redis.script


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ Claim, Claimed }
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.codecs.storage.StoredMessage
import homelab.keyedqueue.infrastructure.redis.Namespace
import io.lettuce.core.ScriptOutputType
import zio.*

import java.time.Instant


/**
 * Turn possession of a key into a claim: message in hand, lease running, token issued — `lua/consume.lua`.
 *
 * Runs '''after''' the `BLMOVE` that put the key in this worker's claiming list, since the blocking part
 * cannot live in a script. The worker is therefore a parameter and not an incidental one: it names the
 * claiming list to finish from, and a wrong one would leave the key stranded where nothing looks for it.
 */
object ConsumeScript:

  /**
   * Multi, because a granted claim comes back as a message and three numbers.
   *
   * @return `MULTI`
   */
  def output: ScriptOutputType = ScriptOutputType.MULTI

  /**
   * The claiming list to finish from, and everything granting a claim writes: the key's state, its lease,
   * its fence, the message it moves between, and its attempt count.
   *
   * @param ns the queue being claimed from
   * @param worker the identity whose claiming list holds the key
   * @param key the key already taken by `BLMOVE`
   * @return `claiming`, `state`, `claimed`, `fence`, `msgs`, `inflight`, `attempts`, in the order
   *         `lua/consume.lua` reads them
   */
  def keys(ns: Namespace, worker: WorkerId, key: MessageKey): Array[String] =
    Array(ns.claiming(worker), ns.state, ns.claimed, ns.fence, ns.msgs(key), ns.inflight(key), ns.attempts)

  /**
   * The key being claimed, and how long its lease should run.
   *
   * @param key the key already taken by `BLMOVE`
   * @param leaseTtl how long the resulting claim survives without a heartbeat
   * @return `key`, `ttl`, in the order `lua/consume.lua` reads them
   */
  def args(key: MessageKey, leaseTtl: Duration): Array[Array[Byte]] =
    Array(LuaScript.utf8(key), LuaScript.utf8(leaseTtl.toMillis.toString))

  /**
   * Decide what the reply was: nothing left on the key, or a claim.
   *
   * The stored bytes are read back here, so an unreadable message fails the claim rather than travelling one
   * layer further as bytes nobody above this adapter should have to think about — and because reading them
   * already yields an `Either`, it composes into the decoder rather than sitting after it.
   *
   * The queue and the key are parameters because the reply does not carry them, and a [[Claim]] needs
   * both to name what was claimed.
   *
   * @param ns the queue being claimed from
   * @param key the key that was claimed
   * @param value the raw reply
   * @return the claim, or `None` when the key turned out to have nothing left; `MalformedReply` when the
   *         reply, or the message in it, cannot be read
   */
  def read(ns: Namespace, key: MessageKey)(value: Any): Either[QueueError, Option[Claimed]] =
    decoder(ns, key).decode("consume", value)

  /**
   * The shape the script promises: `{message, token, attempt, deadline}` when it granted a claim, and
   * absence when the key had nothing left.
   *
   * Absence is the outermost layer rather than something the four-element shape allows, because those are
   * two different replies: `nil` or an empty array is an answer, and anything else has to be all four.
   *
   * The queue and the key are closed over because the reply carries neither, and a [[Claim]] needs both
   * to name what was claimed.
   *
   * @param ns the queue being claimed from
   * @param key the key that was claimed
   * @return the decoder
   */
  private def decoder(ns: Namespace, key: MessageKey): LuaScript.Decode.Of[Option[Claimed]] =
    LuaScript.Decode
      .sized(4) {
        for
          message  <- LuaScript.Decode.bytes.at(0).emap(StoredMessage.fromBytes)
          token    <- LuaScript.Decode.long.at(1)
          attempt  <- LuaScript.Decode.long.at(2)
          deadline <- LuaScript.Decode.long.at(3)
        yield Claimed(
          Claim(ns.queue, key, Token(token)),
          message,
          attempt.toInt,
          Instant.ofEpochMilli(deadline),
        )
      }
      .orNone
