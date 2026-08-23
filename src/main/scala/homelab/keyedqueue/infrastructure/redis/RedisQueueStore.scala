package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ ClaimRef, Claimed }
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.sync.RedisCommands
import zio.*

import java.time.Instant


/**
 * The queue over Redis.
 *
 * '''Two kinds of connection, and the difference is not the caller's business.''' Claiming parks in
 * `BLMOVE`, so it happens on a connection borrowed from [[ClaimerPool]]; everything else runs on a shared
 * connection that must never block, because a blocking command there would stall every other caller on it.
 * Both live behind one `QueueStore`, so a use case holds a store like any other and learns nothing about
 * connections.
 *
 * '''Every operation is one script.''' The interleavings between reading a key's state and acting on it are
 * exactly the bugs this design exists to avoid, so nothing here is a sequence of commands — see
 * `docs/research/redis-keyed-queue.md`.
 *
 * @param redis the shared, never-blocking connection
 * @param scripts the loaded script digests
 * @param claimers the connections that may block
 * @param worker the identity this instance writes when it renews on a caller's behalf
 * @param leaseTtl how long a claim survives without a heartbeat
 */
final class RedisQueueStore(
  redis: RedisCommands[String, Array[Byte]],
  scripts: Scripts,
  claimers: ClaimerPool,
  worker: WorkerId,
  leaseTtl: Duration,
) extends QueueStore:

  /**
   * One `produce` call. The conditional push inside it is what keeps a key in `ready` at most once, and so
   * cannot be split into a read and a write here.
   *
   * @return the key's depth after the append
   */
  override def enqueue(queue: QueueName, key: MessageKey, payload: Chunk[Byte]): IO[QueueError, Long] =
    val ns = Namespace(queue)
    call(scripts.produce, ScriptOutputType.INTEGER, Array(ns.ready, ns.state, ns.msgs(key)), Array(Lua.utf8(key), payload.toArray))
      .flatMap(Lua.number("produce"))

  /**
   * The only operation that can occupy a connection, so the only one that borrows.
   *
   * `BLMOVE` parks for the whole wait, which is why claiming runs on a pooled connection while everything
   * else shares one: the pool's size is the ceiling on concurrent claims, and a caller that cannot get a
   * connection waits for a peer rather than opening another.
   *
   * @return the claim, or `None` on timeout
   */
  override def claim(queue: QueueName, timeout: Duration): IO[QueueError, Option[Claimed]] =
    claimers.borrow(_.claim(queue, timeout))

  /**
   * One `complete` call, which advances the key's fence as well as applying the verdict — so a replayed
   * settle finds its token spent and reports stale rather than acting twice.
   *
   * @return whether it applied
   */
  override def settle(claim: ClaimRef, verdict: Verdict, retryAfter: Duration): IO[QueueError, Boolean] =
    val ns = Namespace(claim.queue)
    call(
      scripts.complete,
      ScriptOutputType.INTEGER,
      Array(ns.state, ns.claimed, ns.fence, ns.msgs(claim.key), ns.inflight(claim.key), ns.ready, ns.attempts, ns.delayed),
      Array(
        Lua.utf8(claim.key),
        Lua.utf8(claim.token.toString),
        Lua.utf8(if verdict == Verdict.Done then "done" else "failed"),
        Lua.utf8(retryAfter.toMillis.toString),
      ),
    ).flatMap(Lua.number("complete")).map(_ == 1L)

  /**
   * One `heartbeat` call '''per queue''', because worker liveness and claims are namespaced by queue while a
   * caller's receipts are not: a consumer holding work in three queues costs three round trips here.
   *
   * @return the new deadline and the claims already revoked
   */
  override def renew(claims: Chunk[ClaimRef]): IO[QueueError, (Instant, Chunk[ClaimRef])] =
    ZIO
      .foreach(claims.groupBy(_.queue).toList)((queue, held) => renewOne(Namespace(queue), held))
      .map: results =>
        (results.map(_._1).maxOption.getOrElse(Instant.EPOCH), Chunk.fromIterable(results).flatMap(_._2))

  /**
   * One `watchdog` call, which carries all three sweeps — lapsed claims, dead workers, elapsed backoffs — so
   * a repair pass is a single round trip and a single blocking window on the server.
   *
   * @return what it repaired
   */
  override def sweep(queue: QueueName, limit: Int): IO[QueueError, QueueStore.Swept] =
    val ns = Namespace(queue)
    call(
      scripts.watchdog,
      ScriptOutputType.MULTI,
      Array(ns.claimed, ns.state, ns.ready, ns.fence, ns.workers, ns.delayed),
      Array(Lua.utf8(limit.toString), Lua.utf8(ns.prefix)),
    ).flatMap:
      case values: java.util.List[?] if values.size == 3 =>
        ZIO.succeed(
          QueueStore.Swept(
            Lua.strings(values.get(0)).map(MessageKey.apply),
            Lua.strings(values.get(1)).map(WorkerId.apply),
            Lua.strings(values.get(2)).map(MessageKey.apply),
          )
        )
      case other                                         => ZIO.fail(QueueError.MalformedReply(s"watchdog returned ${Lua.describe(other)}"))

  /**
   * Renew the claims a caller holds in one queue.
   *
   * This is renewal on behalf of a *consumer*, which is a different thing from a claimer keeping its own
   * registration alive: the deadlines pushed forward are the keys', and the worker entry written is only
   * this instance's own liveness.
   *
   * @param ns the queue
   * @param held the claims to renew, all in that queue
   * @return the new deadline and the claims that had already been revoked
   */
  private def renewOne(ns: Namespace, held: Chunk[ClaimRef]): IO[QueueError, (Instant, Chunk[ClaimRef])] =
    val pairs = held.flatMap(claim => Chunk(Lua.utf8(claim.key), Lua.utf8(claim.token.toString)))
    call(
      scripts.heartbeat,
      ScriptOutputType.MULTI,
      Array(ns.claimed, ns.fence, ns.workers),
      (Chunk(Lua.utf8(leaseTtl.toMillis.toString), Lua.utf8(worker)) ++ pairs).toArray,
    ).flatMap:
      case values: java.util.List[?] if values.size == 2 =>
        val until = values.get(0) match
          case number: java.lang.Long => number.longValue
          case _                      => 0L
        val lost  = Lua.strings(values.get(1)).toSet
        ZIO.succeed((Instant.ofEpochMilli(until), held.filter(claim => lost.contains(claim.key))))
      case other                                         => ZIO.fail(QueueError.MalformedReply(s"heartbeat returned ${Lua.describe(other)}"))

  /**
   * Run a script on the shared connection.
   *
   * Exists only so the operations above do not repeat `redis` in every call, and so it is obvious at a
   * glance which of them use the shared connection rather than a borrowed one.
   *
   * @param sha the digest from [[Scripts]]
   * @param output what shape of reply to expect
   * @param keys the KEYS, in the order the script reads them
   * @param args the ARGV
   * @return the raw reply; aborts with `QueueError` when the store fails
   */
  private def call(sha: String, output: ScriptOutputType, keys: Array[String], args: Array[Array[Byte]]) =
    Lua.call(redis, sha, output, keys, args)
