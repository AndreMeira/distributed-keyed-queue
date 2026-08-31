package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ Claim, Claimed, Demand, Settlement, Submission }
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.infrastructure.codecs.storage.StoredMessage
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.RedisQueueStore.make
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import io.lettuce.core.LMoveArgs
import zio.*

import java.time.Instant


/**
 * The queue over Redis.
 *
 * '''Two kinds of connection, and the difference is not the caller's business.''' Claiming parks in
 * `BLMOVE`, so it borrows a connection of its own through [[Connection.provideBlocking]]; everything else
 * runs on the shared connection, which must never block, because a blocking command there would stall every
 * other caller on it. Both live behind one `QueueStore`, so a use case holds a store like any other and
 * learns nothing about connections.
 *
 * '''Every operation is one script.''' The interleavings between reading a key's state and acting on it are
 * exactly the bugs this design exists to avoid, so nothing here is a sequence of commands — see
 * `docs/research/redis-keyed-queue.md`.
 *
 * '''It remembers which connection announced itself where.''' A claiming connection must be in a queue's
 * worker set before its first `BLMOVE` there, or a key it dies holding can never be recovered — so the
 * store keeps the `(worker, queue)` pairs it has registered and [[beat]] keeps every one of them alive.
 *
 * @param connection where its connections come from, shared or borrowed
 * @param scripts the loaded script digests
 * @param worker the identity this instance writes when it renews on a caller's behalf
 * @param leaseTtl how long a claim, and a registration, survive without a heartbeat
 * @param known the queues each claiming connection has registered in
 */
final class RedisQueueStore(
  connection: Connection,
  scripts: Scripts,
  worker: WorkerId,
  leaseTtl: Duration,
  known: Ref[Set[(WorkerId, QueueName)]],
) extends QueueStore:

  /**
   * One `produce` call. The conditional push inside it is what keeps a key in `ready` at most once, and so
   * cannot be split into a read and a write here.
   *
   * The message is serialised here rather than by the caller: what a message looks like at rest is this
   * adapter's choice — see [[StoredMessage]].
   *
   * @param submission the queue to append in, and the message; the key it carries decides where it lands
   * @return the key's depth after the append
   */
  override def enqueue(submission: Submission): IO[QueueError, Long] =
    connection.provide:
      scripts.produce.run(Namespace(submission.queue), submission.message)

  /**
   * The only operation that can occupy a connection, so the only one that borrows.
   *
   * `BLMOVE` parks for the whole wait, which is why claiming runs on a pooled connection while everything
   * else shares one: the pool's size is the ceiling on concurrent claims, and a caller that cannot get a
   * connection waits for a peer rather than opening another.
   *
   * @param demand the queue to claim from, how long to wait, and the most to take
   * @return the claim, or `None` on timeout
   */
  override def claim(demand: Demand): IO[QueueError, Option[Claimed]] =
    val ns = Namespace(demand.queue)
    connection.provideBlocking: claimer =>
      register(ns, claimer) *> ZIO
        .uninterruptibleMask: restore =>
          restore(take(ns, claimer, demand.patience)).flatMap:
            case None      => ZIO.none
            case Some(key) => scripts.consume.run(ns, claimer, MessageKey(key), leaseTtl, demand.batch)
        .onInterrupt(release(ns, claimer))

  /**
   * One `complete` call, which checks the token every time and advances it only when the claim ends — a
   * partial settle has to leave the receipt usable for the rest of the batch.
   *
   * So it is not the fence that makes a replay harmless here: settling removes the id from the claim's
   * owned set, and removing it a second time finds nothing. The fence is what invalidates the receipt
   * once the claim is over, or once the watchdog has revoked it.
   *
   * @param settlement the claim, what became of the messages it names, and any backoff
   * @return whether it applied
   */
  override def settle(settlement: Settlement): IO[QueueError, Boolean] =
    connection.provide:
      scripts.complete.run(settlement)

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
  override def renew(claims: Chunk[Claim]): IO[QueueError, (Instant, Chunk[Claim])] =
    connection.provide:
      ZIO
        .foreach(claims.groupBy(_.queue).toList): (queue, held) =>
          scripts.heartbeat.run(Namespace(queue), worker, leaseTtl, held)
        .map: results =>
          val (when, chunk) = results.unzip
          when.maxOption.getOrElse(Instant.EPOCH) -> Chunk.fromIterable(chunk).flatten

  /**
   * One `watchdog` call, which carries all three sweeps — lapsed claims, dead workers, elapsed backoffs — so
   * a repair pass is a single round trip and a single idle window on the server.
   *
   * @param queue the queue to repair
   * @param limit the most entries to handle in one pass
   * @return what it repaired
   */
  override def sweep(queue: QueueName, limit: Int): IO[QueueError, QueueStore.Swept] =
    connection.provide:
      scripts.watchdog.run(Namespace(queue), limit)

  /**
   * Renew every registration this store is keeping alive.
   *
   * '''On the shared connection, for every worker.''' `lua/heartbeat.lua` takes the worker as an argument
   * and touches only the queue's own keys, so a beat needs no connection in particular — and running them
   * here keeps them from queueing behind a `BLMOVE` parked on a claiming connection.
   *
   * @return noop; a failed beat is ignored because the next one is moments away
   */
  def beat: UIO[Unit] =
    known.get.flatMap: pairs =>
      ZIO.foreachDiscard(pairs): (claimer, queue) =>
        connection.provide(scripts.heartbeat.run(Namespace(queue), claimer, leaseTtl, Chunk.empty)).ignore

  /**
   * Announce a claiming connection in a queue, once.
   *
   * '''Before its first `BLMOVE` in that queue, and per worker.''' A connection that claims before it is
   * known has a claiming list with no liveness entry to expire, so nothing would ever recover a key it dies
   * holding. The memo is keyed by worker as well as by queue for the same reason: one connection
   * registering does not make its peers recoverable.
   *
   * @param ns the queue to register in
   * @param claimer the identity of the connection about to claim
   * @return noop; aborts with `QueueError` when the store fails
   */
  private def register(ns: Namespace, claimer: WorkerId): ZIO[Connection.Commands, QueueError, Unit] =
    known.get.map(_.contains((claimer, ns.queue))).flatMap {
      case true  => ZIO.unit
      case false =>
        scripts.heartbeat.run(ns, claimer, leaseTtl, Chunk.empty)
          *> known.update(_ + ((claimer, ns.queue))).unit
    }

  /**
   * Take a claimable key into this connection's own claiming list.
   *
   * @param ns the queue to take from
   * @param claimer the identity whose claiming list to move into
   * @param timeout how long to wait
   * @return the key, or `None` on timeout; aborts with `QueueError` when the store fails
   */
  private def take(ns: Namespace, claimer: WorkerId, timeout: Duration): ZIO[Connection.Commands, QueueError, Option[String]] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking {
          Option:
            val move = LMoveArgs.Builder.leftRight()
            val wait = timeout.toMillis.toDouble / 1000.0
            redis.blmove(ns.ready, ns.claiming(claimer), move, wait)
        }
        .mapBoth(LuaScript.failure, _.map(LuaScript.text))

  /**
   * Hand back whatever this connection was holding mid-claim — at most one key, since a borrowed connection
   * makes one `BLMOVE` at a time.
   *
   * @param ns the queue
   * @param claimer the identity whose claiming list to drain
   * @return noop; failures are ignored, because the sweep repairs what this missed
   */
  private def release(ns: Namespace, claimer: WorkerId): URIO[Connection.Commands, Unit] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking:
          val move = LMoveArgs.Builder.rightLeft()
          redis.lmove(ns.claiming(claimer), ns.ready, move)
        .ignore
        .unit


object RedisQueueStore:

  /**
   * The store, with its registrations kept alive for the life of the scope.
   *
   * The beat is started here rather than in the composition root because it is a property of the store: a
   * claiming connection that stops announcing itself becomes unrecoverable, whether or not anything else is
   * running.
   *
   * @param connection where its connections come from
   * @param scripts the loaded digests
   * @param worker the identity it writes when it renews on a caller's behalf
   * @param leaseTtl how long a claim, and a registration, survive without a heartbeat
   * @return the store
   */
  def make(
    connection: Connection,
    scripts: Scripts,
    worker: WorkerId,
    leaseTtl: Duration,
  ): ZIO[Scope, Nothing, RedisQueueStore] =
    for
      known   <- Ref.make(Set.empty[(WorkerId, QueueName)])
      store    = RedisQueueStore(connection, scripts, worker, leaseTtl, known)
      schedule = Schedule.fixed(Duration.fromMillis(leaseTtl.toMillis / 3))
      _       <- store.beat.repeat(schedule).forkScoped
    yield store
