package homelab.keyedqueue.infrastructure.redis.script


import homelab.keyedqueue.domain.model.{ Claim, Grant, Message }
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.codecs.storage.StoredMessage
import homelab.keyedqueue.infrastructure.redis.{ Connection, Namespace, RedisFailure }
import io.lettuce.core.ScriptOutputType
import zio.*

import java.time.Instant


/**
 * Take the next claimable key and claim a batch of its messages — `lua/claim.lua`.
 *
 * '''The whole claim, including choosing the key.''' The script pops `ready` itself, so no key is ever out
 * of the queue and unclaimed — and the reply has to name the key, because the caller does not choose it.
 *
 * @param ref the digest this script was loaded under, from [[Scripts]]
 */
final class ClaimScript(ref: LuaScript.Sha):

  /** Multi, because a claim comes back as three numbers and three arrays. */
  private val output: ScriptOutputType = ScriptOutputType.MULTI

  /**
   * Claim the next key with work, if there is one.
   *
   * Does not block: nothing claimable answers `None` at once, and waiting for something to become
   * claimable is the caller's business.
   *
   * @param ns the queue to claim from
   * @param leaseTtl how long the resulting claim survives without a heartbeat
   * @param maxBatch the most messages to take at once
   * @return the claim, or `None` when nothing was claimable; aborts with `RedisFailure` when the store fails
   *         or the reply cannot be read
   */
  def run(ns: Namespace, leaseTtl: Duration, maxBatch: Int): ZIO[Connection.Commands, RedisFailure, Option[Grant]] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.evalsha[Any](ref, output, keys(ns), args(ns, leaseTtl, maxBatch)*))
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(read(ns)(reply)))

  /**
   * Where a claim comes from and everything it writes: the queue's claimable set, the key's lease,
   * its fence, and the delivery counts.
   *
   * The key's own structures — `msgs`, `payloads`, `owned` — are absent: their names depend on the key the
   * script chooses, and it builds them from the prefix. Legal because all of them share the queue's hash
   * tag, and so its slot.
   *
   * @param ns the queue to claim from
   * @return `ready`, `claimed`, `fence`, `attempts`, in the order `lua/claim.lua` reads them
   */
  private def keys(ns: Namespace): Array[String] =
    Array(ns.ready, ns.claimed, ns.fence, ns.attempts)

  /**
   * The namespace to build the key's own structures from, how long the lease should run, and how many
   * messages to take.
   *
   * @param ns the queue to claim from
   * @param leaseTtl how long the claim survives without a heartbeat
   * @param maxBatch the most messages to take at once
   * @return `prefix`, `ttl`, `batch`, in the order `lua/claim.lua` reads them
   */
  private def args(ns: Namespace, leaseTtl: Duration, maxBatch: Int): Array[Array[Byte]] =
    Array(
      LuaScript.utf8(ns.prefix),
      LuaScript.utf8(leaseTtl.toMillis.toString),
      LuaScript.utf8(maxBatch.toString),
    )

  /**
   * Decide what the reply was: nothing left on the key, or a claim over some of its messages.
   *
   * The stored bytes are read back here, so an unreadable message fails the claim.
   *
   * @param ns the queue being claimed from
   * @param value the raw reply
   * @return the claim, or `None` when nothing was claimable; `MalformedReply` when the reply, or a message
   *         in it, cannot be read
   */
  private def read(ns: Namespace)(value: Any): Either[RedisFailure, Option[Grant]] =
    decoder(ns).decode("consume", value)

  /**
   * Read one stored message, reporting a decoder failure as this adapter's own.
   *
   * @param bytes the message as it was stored
   * @return the message; fails with `MalformedReply` when the bytes cannot be read
   */
  private def stored(bytes: Chunk[Byte]): Either[RedisFailure, Message] =
    StoredMessage.fromBytes(bytes).left.map(failure => RedisFailure.MalformedReply(failure.message))

  /**
   * Line the three parallel arrays back up into one message each.
   *
   * Bounded by the shortest of the three, so a reply whose arrays disagree in length yields what they agree
   * on rather than throwing.
   *
   * @param ids the message ids, in producer order
   * @param messages their payloads, in the same order
   * @param attempts their delivery counts, in the same order
   * @return one entry per message
   */
  private def owned(ids: Chunk[String], messages: Chunk[Message], attempts: Chunk[Long]): Chunk[Grant.Owned] =
    val size = ids.size.min(messages.size).min(attempts.size)
    Chunk
      .fromIterable(0 until size)
      .map: index =>
        Grant.Owned(MessageId(ids(index)), messages(index), attempts(index).toInt)

  /**
   * A claim over nothing is not a claim.
   *
   * The script answers with absence rather than an empty batch, so an empty one here means the script and
   * this adapter disagree — which is a malformed reply, not a claim of no messages.
   *
   * @param batch what was read out of the reply
   * @return the same messages, known to be at least one
   */
  private def nonEmpty(batch: Chunk[Grant.Owned]): LuaScript.Decode.Of[NonEmptyChunk[Grant.Owned]] =
    NonEmptyChunk.fromChunk(batch) match
      case Some(messages) => LuaScript.Decode.succeed(messages)
      case None           => LuaScript.Decode.fail(RedisFailure.MalformedReply("consume granted a claim over no messages"))

  /**
   * The shape the script promises: `{key, token, deadline, backlog, ids, messages, attempts}` when it
   * granted a claim, and absence when nothing was claimable.
   *
   * The last three are parallel arrays — one entry per message, in producer order — so their alignment is
   * the contract.
   *
   * @param ns the queue being claimed from
   * @return the decoder
   */
  private def decoder(ns: Namespace): LuaScript.Decode.Of[Option[Grant]] =
    LuaScript.Decode
      .sized(7) {
        for
          key      <- LuaScript.Decode.text.at(0)
          token    <- LuaScript.Decode.long.at(1)
          deadline <- LuaScript.Decode.long.at(2)
          backlog  <- LuaScript.Decode.long.at(3)
          ids      <- LuaScript.Decode.text.each.at(4)
          messages <- LuaScript.Decode.bytes.emap(stored).each.at(5)
          attempts <- LuaScript.Decode.long.each.at(6)
          batch    <- nonEmpty(owned(ids, messages, attempts))
        yield Grant(
          Claim(ns.queue, MessageKey(key), Token(token)),
          batch,
          Instant.ofEpochMilli(deadline),
          backlog.toInt,
        )
      }
      .orNone


object ClaimScript:

  /**
   * Register `lua/claim.lua` and hold the digest it was given.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def make: ZIO[Connection.Commands, RedisFailure, ClaimScript] =
    LuaScript.register("lua/claim.lua").map(ClaimScript(_))
