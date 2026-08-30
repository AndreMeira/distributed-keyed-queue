package homelab.keyedqueue.infrastructure.redis.script


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ Claim, Claimed, Message }
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.codecs.storage.StoredMessage
import homelab.keyedqueue.infrastructure.redis.{ Connection, Namespace }
import io.lettuce.core.ScriptOutputType
import zio.*

import java.time.Instant


/**
 * Turn possession of a key into a claim over a batch of its messages — `lua/consume.lua`.
 *
 * Runs '''after''' the `BLMOVE` that put the key in this worker's claiming list, since the blocking part
 * cannot live in a script. The worker is therefore a parameter and not an incidental one: it names the
 * claiming list to finish from, and a wrong one would leave the key stranded where nothing looks for it.
 *
 * @param ref the digest this script was loaded under, from [[Scripts]]
 */
final class ConsumeScript(ref: LuaScript.Sha):

  /** Multi, because a claim comes back as three numbers and three arrays. */
  private val output: ScriptOutputType = ScriptOutputType.MULTI

  /**
   * Claim what `BLMOVE` made available.
   *
   * @param ns the queue being claimed from
   * @param worker the identity whose claiming list holds the key
   * @param key the key already taken by `BLMOVE`
   * @param leaseTtl how long the resulting claim survives without a heartbeat
   * @param maxBatch the most messages to take at once
   * @return the claim, or `None` when the key had nothing left; aborts with `QueueError` when the store
   *         fails or the reply cannot be read
   */
  def run(
    ns: Namespace,
    worker: WorkerId,
    key: MessageKey,
    leaseTtl: Duration,
    maxBatch: Int,
  ): ZIO[Connection.Commands, QueueError, Option[Claimed]] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(
          redis.evalsha[Any](ref, output, keys(ns, worker, key), args(key, leaseTtl, maxBatch)*)
        )
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(read(ns, key)(reply)))

  /**
   * The claiming list to finish from, and everything a claim writes: the key's state, its lease, its fence,
   * its messages and their payloads, what the claim owns, and the delivery counts.
   *
   * @param ns the queue being claimed from
   * @param worker the identity whose claiming list holds the key
   * @param key the key already taken by `BLMOVE`
   * @return `claiming`, `state`, `claimed`, `fence`, `msgs`, `payloads`, `owned`, `attempts`, in the order
   *         `lua/consume.lua` reads them
   */
  private def keys(ns: Namespace, worker: WorkerId, key: MessageKey): Array[String] =
    Array(
      ns.claiming(worker),
      ns.state,
      ns.claimed,
      ns.fence,
      ns.msgs(key),
      ns.payloads(key),
      ns.owned(key),
      ns.attempts,
    )

  /**
   * The key being claimed, how long its lease should run, and how many of its messages to take.
   *
   * @param key the key already taken by `BLMOVE`
   * @param leaseTtl how long the claim survives without a heartbeat
   * @param maxBatch the most messages to take at once
   * @return `key`, `ttl`, `batch`, in the order `lua/consume.lua` reads them
   */
  private def args(key: MessageKey, leaseTtl: Duration, maxBatch: Int): Array[Array[Byte]] =
    Array(
      LuaScript.utf8(key),
      LuaScript.utf8(leaseTtl.toMillis.toString),
      LuaScript.utf8(maxBatch.toString),
    )

  /**
   * Decide what the reply was: nothing left on the key, or a claim over some of its messages.
   *
   * The stored bytes are read back here, so an unreadable message fails the claim rather than travelling one
   * layer further as bytes nobody above this adapter should have to think about.
   *
   * @param ns the queue being claimed from
   * @param key the key that was claimed
   * @param value the raw reply
   * @return the claim, or `None` when the key turned out to have nothing left; `MalformedReply` when the
   *         reply, or a message in it, cannot be read
   */
  private def read(ns: Namespace, key: MessageKey)(value: Any): Either[QueueError, Option[Claimed]] =
    decoder(ns, key).decode("consume", value)

  /**
   * Line the three parallel arrays back up into one message each.
   *
   * Bounded by the shortest of the three, so a reply whose arrays disagree in length yields what they agree
   * on rather than throwing — a malformed reply should fail as a reply, not as a defect.
   *
   * @param ids the message ids, in producer order
   * @param messages their payloads, in the same order
   * @param attempts their delivery counts, in the same order
   * @return one entry per message
   */
  private def owned(ids: Chunk[String], messages: Chunk[Message], attempts: Chunk[Long]): Chunk[Claimed.Owned] =
    val size = ids.size.min(messages.size).min(attempts.size)
    Chunk
      .fromIterable(0 until size)
      .map: index =>
        Claimed.Owned(MessageId(ids(index)), messages(index), attempts(index).toInt)

  /**
   * A claim over nothing is not a claim.
   *
   * The script answers with absence rather than an empty batch, so an empty one here means the script and
   * this adapter disagree — which is a malformed reply, not a claim of no messages.
   *
   * @param batch what was read out of the reply
   * @return the same messages, known to be at least one
   */
  private def nonEmpty(batch: Chunk[Claimed.Owned]): LuaScript.Decode.Of[NonEmptyChunk[Claimed.Owned]] =
    NonEmptyChunk.fromChunk(batch) match
      case Some(messages) => LuaScript.Decode.succeed(messages)
      case None           => LuaScript.Decode.fail(QueueError.MalformedReply("consume granted a claim over no messages"))

  /**
   * The shape the script promises: `{token, deadline, backlog, ids, messages, attempts}` when it granted a
   * claim, and absence when the key had nothing left.
   *
   * The last three are parallel arrays — one entry per message, in producer order — which is why they are
   * zipped rather than read as a list of triples: Lua has no record to return, so the alignment is the
   * contract.
   *
   * @param ns the queue being claimed from
   * @param key the key that was claimed
   * @return the decoder
   */
  private def decoder(ns: Namespace, key: MessageKey): LuaScript.Decode.Of[Option[Claimed]] =
    LuaScript.Decode
      .sized(6) {
        for
          token    <- LuaScript.Decode.long.at(0)
          deadline <- LuaScript.Decode.long.at(1)
          backlog  <- LuaScript.Decode.long.at(2)
          ids      <- LuaScript.Decode.text.each.at(3)
          messages <- LuaScript.Decode.bytes.emap(StoredMessage.fromBytes).each.at(4)
          attempts <- LuaScript.Decode.long.each.at(5)
          batch    <- nonEmpty(owned(ids, messages, attempts))
        yield Claimed(
          Claim(ns.queue, key, Token(token)),
          batch,
          Instant.ofEpochMilli(deadline),
          backlog.toInt,
        )
      }
      .orNone


object ConsumeScript:

  /**
   * Register `lua/consume.lua` and hold the digest it was given.
   *
   * @return the script, ready to run; aborts with `QueueError` if it is missing or the server rejects it
   */
  def make: ZIO[Connection.Commands, QueueError, ConsumeScript] =
    LuaScript.register("lua/consume.lua").map(ConsumeScript(_))
