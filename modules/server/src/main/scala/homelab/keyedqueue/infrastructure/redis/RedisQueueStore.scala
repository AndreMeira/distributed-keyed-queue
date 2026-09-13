package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.model.{ Claim, Demand, Grant, Settlement, Submission }
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.infrastructure.codecs.storage.StoredMessage
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.RedisQueueStore.make
import homelab.keyedqueue.infrastructure.redis.keys.QueueKeys
import homelab.keyedqueue.infrastructure.redis.script.{ LuaScript, QueueScripts }
import homelab.keyedqueue.infrastructure.redis.script.queue.{ ClaimScript, RenewScript }
import io.lettuce.core.LMoveArgs
import zio.*

import java.time.Instant


/**
 * The queue over Redis.
 *
 * '''Nothing here blocks in Redis.''' Every operation is a script that answers at once, so they all share
 * one connection. The only command in the process that parks is the listener's `XREAD`, and that belongs to
 * [[ReadinessListener]], on a connection of its own.
 *
 * '''Every operation is one script.''' The interleavings between reading a key's state and acting on it are
 * exactly the bugs this design exists to avoid, so nothing here is a sequence of commands — see
 * `docs/research/redis-keyed-queue.md`.
 *
 * '''There is nothing to keep alive.''' A claim is granted in one call, so a key is either queued or
 * claimed and the lease is the only thing that expires. No connection announces itself, and no registration
 * has to be renewed on its behalf.
 *
 * '''Traced, not measured.''' Everything that reaches the substrate opens a span; none of it records a
 * hit or a latency series. The RPC above already counts and times the operation a caller asked for, so a
 * second metric per store method would double the series for something the span already answers — and the
 * question these are here for is "where did that call's time go", which is a trace question. The pure
 * helpers are left alone: a span around clock arithmetic is noise.
 *
 * `attempt` is traced although it is private, because it is the one that repeats: a claim that loses the
 * race retries, and the span count is what shows the redundant attempts the design pays in.
 *
 * @param monitor what each call on the substrate is traced against
 * @param connection where its connection comes from
 * @param scripts the loaded script digests
 * @param readiness where a caller waits for a queue to have something worth looking at
 * @param leaseTtl how long a claim survives without a heartbeat
 */
final class RedisQueueStore(
  monitor: Monitor,
  connection: Connection,
  scripts: QueueScripts,
  readiness: QueueReadiness,
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
        scripts.enqueue.execute(QueueKeys(submission.queue), submission.message)

  /**
   * One `claim` call, and — when it finds nothing — a wait for the queue to be worth another look.
   *
   * '''The claim is a single script, so nothing blocks in Redis.''' A key is either in `ready` or claimed;
   * there is no instant in which it is neither, which is why this adapter has no holding list, no
   * per-connection identity and no recovery for one.
   *
   * '''Patience is a deadline.''' A caller woken by an entry another instance won keeps waiting with what
   * is left of it, rather than starting again — so a race it loses costs it a round trip, not a full wait.
   *
   * @param demand the queue to claim from, how long to wait, and the most to take
   * @return the claim, or `None` when the patience elapsed; aborts with `RedisFailure` when the store fails
   */
  override def claim(demand: Demand): IO[RedisFailure, Option[Grant]] =
    monitor.trace("RedisQueueStore.claim"):
      for
        asked   <- Clock.instant
        claimed <- claimWithin(QueueKeys(demand.queue), demand, asked)
      yield claimed

  /**
   * Wait for a readiness token, claim when one arrives, and keep at it until the patience is spent.
   *
   * @param ns the queue being claimed from
   * @param demand what the caller asked for
   * @param asked when its call arrived, which is what the patience is measured from
   * @return the claim, or `None` when the patience elapsed; aborts with `RedisFailure` when the store fails
   */
  private def claimWithin(ns: QueueKeys, demand: Demand, asked: Instant): IO[RedisFailure, Option[Grant]] =
    remainingTime(demand.patience, asked).flatMap {
      case None               => ZIO.none
      case Some(patienceLeft) =>
        readiness
          .awaitReady(demand.queue, patienceLeft):
            attemptClaim(ns, demand)
          .flatMap:
            case granted @ Some(_) => ZIO.succeed(granted)
            case None              => claimWithin(ns, demand, asked)
    }

  /**
   * Claim whatever is claimable, without waiting.
   *
   * @param ns the queue to claim from
   * @param demand how much to take
   * @return the claim, or `None` when nothing was claimable; aborts with `RedisFailure` when the store fails
   */
  private def attemptClaim(ns: QueueKeys, demand: Demand): IO[RedisFailure, Option[Grant]] =
    monitor.trace("RedisQueueStore.attemptClaim"):
      connection.provide:
        scripts.claim.execute(ns, leaseTtl, demand.batch).map(_.map(granted(ns)))

  /**
   * The claim a granted reply amounts to.
   *
   * The reply names the key the script chose; only this caller knows which queue it was chosen from, which
   * is why the script answers with less than a `Grant`.
   *
   * @param ns the queue that was claimed from
   * @param claimed what the script granted
   * @return the claim, as the port promises it
   */
  private def granted(ns: QueueKeys)(claimed: ClaimScript.Claimed): Grant =
    Grant(Claim(ns.queue, claimed.key, claimed.token), claimed.batch, claimed.deadline, claimed.backlog)

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
  private def renewed(held: Chunk[Claim])(reply: RenewScript.Output): (Instant, Chunk[Claim]) =
    (reply.renewedUntil, held.filter(claim => reply.lost.toSet.contains(claim.key)))

  /**
   * What is left of a caller's patience.
   *
   * @param patience what the caller was granted
   * @param asked when its call reached this adapter
   * @return the time still to wait, or `None` when the patience is already spent
   */
  private def remainingTime(patience: Duration, asked: Instant): UIO[Option[Duration]] =
    Clock.instant.map: now =>
      val left = patience.minus(Duration.fromInterval(asked, now))
      Option.when(left.toMillis > 0)(left)

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
        scripts.settle.execute(QueueKeys(settlement.claimed.queue), settlement)

  /**
   * One `renew` call '''per queue''', because claims are namespaced by queue while a caller's receipts
   * are not: a consumer holding work in three queues costs three round trips here.
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
            scripts.renew.execute(QueueKeys(queue), leaseTtl, held).map(renewed(held))
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
        scripts.sweep.execute(QueueKeys(queue), limit)


object RedisQueueStore:

  /**
   * The store.
   *
   * Nothing is started here any more: the store holds no registrations to renew, and the streams it waits
   * on is run by whoever owns the listener.
   *
   * @param monitor what each call on the substrate is traced against
   * @param connection where its connection comes from
   * @param scripts the loaded digests
   * @param readiness where a caller waits for a queue to have something worth looking at
   * @param leaseTtl how long a claim survives without a heartbeat
   * @return the store
   */
  def make(
    monitor: Monitor,
    connection: Connection,
    scripts: QueueScripts,
    readiness: QueueReadiness,
    leaseTtl: Duration,
  ): UIO[RedisQueueStore] =
    ZIO.succeed(RedisQueueStore(monitor, connection, scripts, readiness, leaseTtl))
