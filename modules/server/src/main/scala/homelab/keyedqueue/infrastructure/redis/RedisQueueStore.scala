package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ ClaimRef, Claimed, Message }
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.infrastructure.codecs.storage.StoredMessage
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.LuaScript.syntax.*
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
   * The message is serialised here rather than by the caller: what a message looks like at rest is this
   * adapter's choice — see [[StoredMessage]].
   *
   * @param queue the queue to append in
   * @param message the message; the key it carries decides where it lands
   * @return the key's depth after the append
   */
  override def enqueue(queue: QueueName, message: Message): IO[QueueError, Long] =
    redis.execute(scripts.produce(Namespace(queue), message))

  /**
   * The only operation that can occupy a connection, so the only one that borrows.
   *
   * `BLMOVE` parks for the whole wait, which is why claiming runs on a pooled connection while everything
   * else shares one: the pool's size is the ceiling on concurrent claims, and a caller that cannot get a
   * connection waits for a peer rather than opening another.
   *
   * @param queue the queue to claim from
   * @param timeout how long to wait for work
   * @return the claim, or `None` on timeout
   */
  override def claim(queue: QueueName, timeout: Duration): IO[QueueError, Option[Claimed]] =
    claimers.borrow(_.claim(queue, timeout))

  /**
   * One `complete` call, which advances the key's fence as well as applying the verdict — so a replayed
   * settle finds its token spent and reports stale rather than acting twice.
   *
   * @param claim the claim being settled, and the token that authorises it
   * @param verdict what became of the message
   * @param retryAfter how long to hold a failed message back; ignored when the verdict is `Done`
   * @return whether it applied
   */
  override def settle(claim: ClaimRef, verdict: Verdict, retryAfter: Duration): IO[QueueError, Boolean] =
    redis.execute(scripts.complete(claim, verdict, retryAfter))

  /**
   * One `heartbeat` call '''per queue''', because worker liveness and claims are namespaced by queue while a
   * caller's receipts are not: a consumer holding work in three queues costs three round trips here.
   *
   * The claims renewed are the caller's, and the only worker entry written is this instance's own liveness —
   * a different thing from a claimer keeping its registration alive, which it does on its own connection.
   *
   * @param claims every claim the caller still holds, across any number of queues
   * @return the new deadline and the claims already revoked
   */
  override def renew(claims: Chunk[ClaimRef]): IO[QueueError, (Instant, Chunk[ClaimRef])] =
    ZIO
      .foreach(claims.groupBy(_.queue).toList)((queue, held) => redis.execute(scripts.heartbeat(Namespace(queue), worker, leaseTtl, held)))
      .map: results =>
        (results.map(_._1).maxOption.getOrElse(Instant.EPOCH), Chunk.fromIterable(results).flatMap(_._2))

  /**
   * One `watchdog` call, which carries all three sweeps — lapsed claims, dead workers, elapsed backoffs — so
   * a repair pass is a single round trip and a single blocking window on the server.
   *
   * @param queue the queue to repair
   * @param limit the most entries to handle in one pass
   * @return what it repaired
   */
  override def sweep(queue: QueueName, limit: Int): IO[QueueError, QueueStore.Swept] =
    redis.execute(scripts.watchdog(Namespace(queue), limit))
