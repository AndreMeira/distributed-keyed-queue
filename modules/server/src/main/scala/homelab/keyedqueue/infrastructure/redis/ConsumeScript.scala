package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ ClaimRef, Claimed }
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.codecs.storage.StoredMessage
import io.lettuce.core.ScriptOutputType
import zio.*

import java.time.Instant
import java.lang.Long as JLong
import scala.jdk.CollectionConverters.*


/**
 * Turn possession of a key into a claim: message in hand, lease running, token issued — `lua/consume.lua`.
 *
 * Runs '''after''' the `BLMOVE` that put the key in this worker's claiming list, since the blocking part
 * cannot live in a script. The worker is therefore a parameter and not an incidental one: it names the
 * claiming list to finish from, and a wrong one would leave the key stranded where nothing looks for it.
 *
 * @param sha the digest this script was loaded under
 * @param ns the queue being claimed from
 * @param worker the identity whose claiming list holds the key
 * @param key the key already taken by `BLMOVE`
 * @param leaseTtl how long the resulting claim survives without a heartbeat
 */
final case class ConsumeScript(sha: String, ns: Namespace, worker: WorkerId, key: MessageKey, leaseTtl: Duration) extends LuaScript[Option[Claimed]]:

  /**
   * Multi, because a granted claim comes back as a message and three numbers.
   *
   * @return `MULTI`
   */
  override def output: ScriptOutputType = ScriptOutputType.MULTI

  /**
   * The claiming list to finish from, and everything granting a claim writes: the key's state, its lease,
   * its fence, the message it moves between, and its attempt count.
   *
   * @return `claiming`, `state`, `claimed`, `fence`, `msgs`, `inflight`, `attempts`, in the order
   *         `lua/consume.lua` reads them
   */
  override def keys: Array[String] =
    Array(ns.claiming(worker), ns.state, ns.claimed, ns.fence, ns.msgs(key), ns.inflight(key), ns.attempts)

  /**
   * The key being claimed, and how long its lease should run.
   *
   * @return `key`, `ttl`, in the order `lua/consume.lua` reads them
   */
  override def args: Array[Array[Byte]] =
    Array(LuaScript.utf8(key), LuaScript.utf8(leaseTtl.toMillis.toString))

  /**
   * Decide what the reply was: nothing left on the key, or a claim.
   *
   * The stored bytes are read back here, so an unreadable message fails the claim rather than travelling one
   * layer further as bytes nobody above this adapter should have to think about.
   *
   * @param value the raw reply
   * @return the claim, or `None` when the key turned out to have nothing left; aborts with `MalformedReply`
   *         when the reply, or the message in it, cannot be read
   */
  override def read(value: Any): IO[QueueError, Option[Claimed]] = value match
    case null                                        => ZIO.none
    case values: java.util.List[?] if values.isEmpty => ZIO.none
    case values: java.util.List[?]                   => claimed(values)
    case other                                       =>
      ZIO.fail(QueueError.MalformedReply(s"consume returned ${LuaScript.describe(other)}"))

  /**
   * Read a reply that carried something into the claim it describes.
   *
   * The script promises `{message, token, attempt, deadline}`, so arity and element types are matched in one
   * pattern: anything else is a script and an adapter that disagree, which is one failure rather than four
   * partial reads that each go wrong somewhere further down.
   *
   * @param values the reply, already known to be a non-empty list
   * @return the claim; aborts with `MalformedReply` when the reply, or the message in it, cannot be read
   */
  private def claimed(values: java.util.List[?]): IO[QueueError, Option[Claimed]] =
    values.asScala.toList match {
      case (message: Array[Byte]) :: (token: JLong) :: (attempt: JLong) :: (deadline: JLong) :: Nil =>
        ZIO.fromEither(StoredMessage.fromBytes(Chunk.fromArray(message)).map { stored =>
          val ref = ClaimRef(ns.queue, key, Token(token.longValue))
          Some(Claimed(ref, stored, attempt.intValue, Instant.ofEpochMilli(deadline.longValue)))
        })

      case _ => ZIO.fail(QueueError.MalformedReply(s"consume returned ${LuaScript.describe(values)}"))
    }
