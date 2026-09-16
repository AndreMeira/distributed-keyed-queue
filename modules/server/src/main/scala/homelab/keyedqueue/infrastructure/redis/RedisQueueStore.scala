package homelab.keyedqueue.infrastructure.redis


import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.model.{ Claim, Demand, Grant, Settlement, Submission }
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.infrastructure.codecs.storage.StoredMessage
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.keys.{ KeyLayout, QueueKeys }
import homelab.keyedqueue.infrastructure.redis.script.QueueScripts
import homelab.keyedqueue.infrastructure.redis.script.queue.{ ClaimScript, RenewScript }
import zio.*

import java.time.Instant


/**
 * The queue over Redis.
 *
 * Nothing here blocks in Redis: every operation is one script that answers at once, so they all share a
 * single connection. The only command in the process that parks is the wake path's `XREAD`, on connections
 * of its own — see [[WakeConsumer]].
 *
 * A claim is granted in one call, so a key is either queued or claimed and the lease is the only thing that
 * expires: no connection announces itself, and nothing has to be kept alive on its behalf.
 *
 * Everything that reaches the substrate opens a span, and none of it records a metric — the RPC above
 * already counts and times what a caller asked for. `attempt` is traced although it is private, because it
 * is the one that repeats: its span count shows the redundant attempts a lost race pays for.
 *
 * See `docs/architecture/redis-connections.md` and `docs/research/redis-keyed-queue.md`.
 *
 * @param monitor what each call on the substrate is traced against
 * @param connection where its connection comes from
 * @param scripts the loaded script digests
 * @param leaseTtl how long a claim survives without a heartbeat
 */
final class RedisQueueStore(
  monitor: Monitor,
  connection: Connection,
  scripts: QueueScripts,
  layout: KeyLayout,
  leaseTtl: Duration,
) extends QueueStore:

  /**
   * One `enqueue` call. The conditional add inside it is what keeps a key in `ready` at most once, and so
   * cannot be split into a read and a write here.
   *
   * The message is serialised here rather than by the caller: what a message looks like at rest is this
   * adapter's choice — see [[StoredMessage]].
   *
   * @param submission the queue to append in, and the message; the key it carries decides where it lands
   * @return the key's depth after the append
   */
  override def enqueue(submission: Submission): IO[RedisFailure, Long] =
    monitor.trace("RedisQueueStore.enqueue"):
      connection.provide:
        scripts.enqueue.execute(layout.queue(submission.queue), submission.message)

  /**
   * Claim whatever is claimable, without waiting.
   *
   * @param demand how much to take
   * @return the claim, or `None` when nothing was claimable; aborts with `RedisFailure` when the store fails
   */
  override def attemptClaim(demand: Demand): IO[RedisFailure, Option[Grant]] =
    monitor.trace("RedisQueueStore.attemptClaim"):
      connection.provide:
        val keys = layout.queue(demand.queue)
        scripts.claim
          .execute(keys, leaseTtl, demand.batch)
          .map:
            case None          => None
            case Some(claimed) => Some(granted(keys, claimed))

  /**
   * One `settle` call, which checks the token every time and advances it only when the claim ends — a
   * partial settle has to leave the receipt usable for the rest of the batch.
   *
   * So it is not the fence that makes a replay harmless here: settling removes the id from the claim's
   * owned set, and removing it a second time finds nothing. The fence is what invalidates the receipt
   * once the claim is over, or once the watchdog has revoked it.
   *
   * @param settlement the claim, what became of the messages it names, and any backoff
   * @return whether it applied
   */
  override def settle(settlement: Settlement): IO[RedisFailure, Boolean] =
    monitor.trace("RedisQueueStore.settle"):
      connection.provide:
        scripts.settle.execute(layout.queue(settlement.claimed.queue), settlement)

  /**
   * One `renew` call per queue, because claims are namespaced by queue while a caller's receipts are not:
   * a consumer holding work in three queues costs three round trips here.
   *
   * No worker entry is written, because there are none: a consumer is known by its receipts, and its
   * claims are found by fence token.
   *
   * @param claims every claim the caller still holds, across any number of queues
   * @return the new deadline and the claims already revoked
   */
  override def renew(claims: Chunk[Claim]): IO[RedisFailure, (Instant, Chunk[Claim])] =
    monitor.trace("RedisQueueStore.renew"):
      connection.provide:
        ZIO
          .foreach(claims.groupBy(_.queue).toList): (queue, held) =>
            scripts.renew.execute(layout.queue(queue), leaseTtl, held).map(renewed(held, _))
          .map: results =>
            val (when, chunk) = results.unzip
            when.maxOption.getOrElse(Instant.EPOCH) -> Chunk.fromIterable(chunk).flatten

  /**
   * One `sweep` call, which carries both passes — lapsed claims and elapsed backoffs — so
   * a repair pass is a single round trip and a single idle window on the server.
   *
   * @param queue the queue to repair
   * @param limit the most entries to handle in one pass
   * @return what it repaired
   */
  override def sweep(queue: QueueName, limit: Int): IO[RedisFailure, QueueStore.Swept] =
    monitor.trace("RedisQueueStore.sweep"):
      connection.provide:
        scripts.sweep.execute(layout.queue(queue), limit)

  /**
   * The claim a granted reply amounts to.
   *
   * The reply names the key the script chose; only this caller knows which queue it was chosen from, which
   * is why the script answers with less than a `Grant`.
   *
   * @param keys the queue that was claimed from
   * @param claimed what the script granted
   * @return the claim, as the port promises it
   */
  private def granted(keys: QueueKeys, claimed: ClaimScript.Claimed): Grant =
    Grant(Claim(keys.queue, claimed.key, claimed.token), claimed.batch, claimed.deadline, claimed.backlog)

  /**
   * Match the keys a beat could not renew back to the claims it was sent with.
   *
   * The script reports keys, and the caller has to stop working *claims* — a key alone would not say which
   * claim it lost, which is why only the beat that was sent can pair them up.
   *
   * @param held the claims the beat carried
   * @param reply the deadline every renewed claim now carries, and the keys that were not renewed
   * @return the deadline, and the claims that had already been revoked
   */
  private def renewed(held: Chunk[Claim], reply: RenewScript.Output): (Instant, Chunk[Claim]) =
    (reply.renewedUntil, held.filter(claim => reply.lost.toSet.contains(claim.key)))
