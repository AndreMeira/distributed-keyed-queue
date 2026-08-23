package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ ClaimRef, Claimed }
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.{ LMoveArgs, ScriptOutputType }
import zio.*

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.jdk.CollectionConverters.*


/**
 * The queue over Redis, one instance per blocking connection.
 *
 * '''It owns its connection.''' `claim` parks in `BLMOVE`, which occupies the connection for the whole wait,
 * so a claimer is a scarce resource handed out by [[ClaimerPool]] rather than something to create per
 * request.
 *
 * '''Every call is one script.''' The interleavings between reading a key's state and acting on it are
 * exactly the bugs this design exists to avoid, so nothing here is a sequence of commands — see
 * `docs/research/redis-keyed-queue.md`.
 *
 * @param redis this instance's own connection
 * @param scripts the loaded script digests
 * @param worker this connection's identity, registered before it claims anything
 * @param leaseTtl how long a claim survives without a heartbeat
 */
final class RedisQueueStore(
  redis: RedisCommands[String, Array[Byte]],
  scripts: Scripts,
  worker: WorkerId,
  leaseTtl: Duration,
) extends QueueStore:

  override def enqueue(queue: QueueName, key: MessageKey, payload: Chunk[Byte]): IO[QueueError, Long] =
    val ns = Namespace(queue)
    call(scripts.produce, ScriptOutputType.INTEGER, Array(ns.ready, ns.state, ns.msgs(key)),
      Array(utf8(key), payload.toArray))
      .flatMap(number("produce"))

  override def claim(queue: QueueName, timeout: Duration): IO[QueueError, Option[Claimed]] =
    val ns = Namespace(queue)
    ZIO
      .uninterruptibleMask: restore =>
        restore(take(ns, timeout)).flatMap:
          case None      => ZIO.none
          case Some(key) => granted(ns, MessageKey(key))
      .onInterrupt(release(ns))

  override def settle(claim: ClaimRef, verdict: Verdict, retryAfter: Duration): IO[QueueError, Boolean] =
    val ns = Namespace(claim.queue)
    call(
      scripts.complete,
      ScriptOutputType.INTEGER,
      Array(ns.state, ns.claimed, ns.fence, ns.msgs(claim.key), ns.inflight(claim.key), ns.ready,
        ns.attempts, ns.delayed),
      Array(
        utf8(claim.key),
        utf8(claim.token.toString),
        utf8(if verdict == Verdict.Done then "done" else "failed"),
        utf8(retryAfter.toMillis.toString),
      ),
    ).flatMap(number("complete")).map(_ == 1L)

  override def renew(claims: Chunk[ClaimRef]): IO[QueueError, (Instant, Chunk[ClaimRef])] =
    val byQueue = claims.groupBy(_.queue).toList
    val queues  = if byQueue.isEmpty then List((QueueName(""), Chunk.empty[ClaimRef])) else byQueue
    ZIO
      .foreach(queues)((queue, held) => renewOne(Namespace(queue), held))
      .map: results =>
        (results.map(_._1).maxOption.getOrElse(Instant.EPOCH), Chunk.fromIterable(results).flatMap(_._2))

  override def sweep(queue: QueueName, limit: Int): IO[QueueError, QueueStore.Swept] =
    val ns = Namespace(queue)
    call(
      scripts.watchdog,
      ScriptOutputType.MULTI,
      Array(ns.claimed, ns.state, ns.ready, ns.fence, ns.workers, ns.delayed),
      Array(utf8(limit.toString), utf8(ns.prefix)),
    ).flatMap:
      case values: java.util.List[?] =>
        val lists = values.asScala.toList.map(nested)
        ZIO.succeed(
          QueueStore.Swept(
            lists.lift(0).map(_.map(MessageKey.apply)).getOrElse(Chunk.empty),
            lists.lift(1).map(_.map(WorkerId.apply)).getOrElse(Chunk.empty),
            lists.lift(2).map(_.map(MessageKey.apply)).getOrElse(Chunk.empty),
          )
        )
      case other => ZIO.fail(QueueError.MalformedReply(s"watchdog returned ${describe(other)}"))

  /**
   * Register this connection and renew the claims it holds in one queue.
   *
   * Registration and renewal are the same write, which is what makes "heartbeat before the first claim" a
   * rule the code can keep rather than a comment.
   *
   * @param ns the queue whose worker set to write to
   * @param held the claims to renew, all in that queue
   * @return the new deadline and the claims that had already been revoked
   */
  private def renewOne(ns: Namespace, held: Chunk[ClaimRef]): IO[QueueError, (Instant, Chunk[ClaimRef])] =
    val pairs = held.flatMap(claim => Chunk(utf8(claim.key), utf8(claim.token.toString)))
    call(
      scripts.heartbeat,
      ScriptOutputType.MULTI,
      Array(ns.claimed, ns.fence, ns.workers),
      (Chunk(utf8(leaseTtl.toMillis.toString), utf8(worker)) ++ pairs).toArray,
    ).flatMap:
      case values: java.util.List[?] if values.size == 2 =>
        val until = values.get(0) match
          case number: java.lang.Long => number.longValue
          case _                      => 0L
        val lost = nested(values.get(1)).toSet
        ZIO.succeed((Instant.ofEpochMilli(until), held.filter(claim => lost.contains(claim.key))))
      case other => ZIO.fail(QueueError.MalformedReply(s"heartbeat returned ${describe(other)}"))

  /**
   * Take a claimable key, or nothing if none appears in time.
   *
   * The destination is this connection's own claiming list, which is what makes a death here recoverable:
   * the key is attributable to a worker whose liveness is a fact rather than an inference.
   *
   * @param ns the queue to take from
   * @param timeout how long to wait
   * @return the key, or `None` on timeout
   */
  private def take(ns: Namespace, timeout: Duration): IO[QueueError, Option[String]] =
    ZIO
      .attemptBlocking(
        Option(
          redis.blmove(ns.ready, ns.claiming(worker), LMoveArgs.Builder.leftRight(),
            timeout.toMillis.toDouble / 1000.0)
        )
      )
      .mapBoth(failure, _.map(text))

  /**
   * Turn possession of a key into a claim: message in hand, lease running, token issued.
   *
   * @param ns the queue
   * @param key the key taken by [[take]]
   * @return the claim, or `None` when the key turned out to have nothing left
   */
  private def granted(ns: Namespace, key: MessageKey): IO[QueueError, Option[Claimed]] =
    call(
      scripts.consume,
      ScriptOutputType.MULTI,
      Array(ns.claiming(worker), ns.state, ns.claimed, ns.fence, ns.msgs(key), ns.inflight(key), ns.attempts),
      Array(utf8(key), utf8(leaseTtl.toMillis.toString)),
    ).flatMap:
      case null                                              => ZIO.none
      case values: java.util.List[?] if values.isEmpty       => ZIO.none
      case values: java.util.List[?] if values.size == 4     =>
        val payload  = values.get(0) match
          case bytes: Array[Byte] => Chunk.fromArray(bytes)
          case _                  => Chunk.empty[Byte]
        val numbers  = List(1, 2, 3).map(values.get(_)).collect { case number: java.lang.Long => number.longValue }
        numbers match
          case List(token, attempt, deadline) =>
            ZIO.some(
              Claimed(ClaimRef(ns.queue, key, Token(token)), payload, attempt.toInt,
                Instant.ofEpochMilli(deadline))
            )
          case _ => ZIO.fail(QueueError.MalformedReply(s"consume returned ${describe(values)}"))
      case other => ZIO.fail(QueueError.MalformedReply(s"consume returned ${describe(other)}"))

  /**
   * Hand back whatever this connection was holding mid-claim.
   *
   * At most one key, since it makes one `BLMOVE` at a time. It matters because a cancelled request cannot
   * un-send a network call: the key may already have moved server-side. Worker liveness is the backstop for
   * a crash, but a live instance never expires, so it must clean up after itself.
   *
   * @param ns the queue
   * @return noop; failures are ignored, because the sweep repairs what this missed
   */
  private def release(ns: Namespace): UIO[Unit] =
    ZIO
      .attemptBlocking(redis.lmove(ns.claiming(worker), ns.ready, LMoveArgs.Builder.rightLeft()))
      .ignore
      .unit

  /**
   * Call a loaded script.
   *
   * @param sha the digest from [[Scripts]]
   * @param output what shape of reply to expect
   * @param keys the KEYS, in the order the script reads them
   * @param args the ARGV
   * @return the raw reply; aborts with `QueueError` if the store fails
   */
  private def call(
    sha: String,
    output: ScriptOutputType,
    keys: Array[String],
    args: Array[Array[Byte]],
  ): IO[QueueError, Any] =
    ZIO.attemptBlocking(redis.evalsha[Any](sha, output, keys, args*)).mapError(failure)

  private def utf8(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

  private def text(value: Array[Byte]): String = String(value, StandardCharsets.UTF_8)

  private def nested(value: Any): Chunk[String] = value match
    case values: java.util.List[?] =>
      Chunk.fromIterable(values.asScala.toList).collect { case bytes: Array[Byte] => text(bytes) }
    case _ => Chunk.empty

  private def number(context: String)(value: Any): IO[QueueError, Long] = value match
    case number: java.lang.Long => ZIO.succeed(number.longValue)
    case other                  => ZIO.fail(QueueError.MalformedReply(s"$context: expected an integer, got ${describe(other)}"))

  private def describe(value: Any): String = if value == null then "nil" else value.getClass.getName

  private def failure(error: Throwable): QueueError = QueueError.StoreUnavailable(error.getMessage)
