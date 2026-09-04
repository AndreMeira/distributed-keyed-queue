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
 * '''Nothing here blocks in Redis.''' Every operation is a script that answers at once, so they all share
 * one connection. The only command in the process that parks is the listener's `XREAD`, and that belongs to
 * [[WakeListener]], on a connection of its own.
 *
 * '''Every operation is one script.''' The interleavings between reading a key's state and acting on it are
 * exactly the bugs this design exists to avoid, so nothing here is a sequence of commands — see
 * `docs/research/redis-keyed-queue.md`.
 *
 * '''There is nothing to keep alive.''' A claim is granted in one call, so a key is either queued or
 * claimed and the lease is the only thing that expires. No connection announces itself, and no registration
 * has to be renewed on its behalf.
 *
 * @param connection where its connection comes from
 * @param scripts the loaded script digests
 * @param waiters where a caller waits when there is nothing to claim
 * @param leaseTtl how long a claim survives without a heartbeat
 */
final class RedisQueueStore(
  connection: Connection,
  scripts: Scripts,
  waiters: Waiters,
  leaseTtl: Duration,
  buckets: Int,
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
      scripts.produce.run(Namespace(submission.queue, buckets), submission.message)

  /**
   * One `consume` call, and — when it finds nothing — a wait on the queue's signal.
   *
   * '''The claim is a single script, so nothing blocks in Redis.''' A key is either in `ready` or claimed;
   * there is no instant in which it is neither, which is why this adapter has no holding list, no
   * per-connection identity and no recovery for one.
   *
   * '''Waiting costs a fiber, not a connection.''' The listener reads every wake stream in the deployment
   * on one connection, and raises the signal every caller waiting on the named queue is holding.
   *
   * '''Patience is a deadline.''' A caller woken by an entry another instance won keeps waiting with what
   * is left of it, rather than starting again — so a race it loses costs it a round trip, not a full wait.
   *
   * @param demand the queue to claim from, how long to wait, and the most to take
   * @return the claim, or `None` when the patience elapsed; aborts with `QueueError` when the store fails
   */
  override def claim(demand: Demand): IO[QueueError, Option[Claimed]] =
    for
      asked   <- Clock.instant
      claimed <- claimWithin(Namespace(demand.queue, buckets), demand, asked)
    yield claimed

  /**
   * Take a signal, look, and — finding nothing — wait on it, until the patience is spent.
   *
   * '''The signal is taken before the look, and that ordering is the whole mechanism.''' A wake that lands
   * while the claim attempt is in flight completes the signal this caller is already holding, so it is
   * waited on and returns immediately. Looking first and subscribing after would drop exactly that wake,
   * and the consumer would sleep out its patience beside claimable work.
   *
   * A wake is a hint, not a handover: every instance reads the same entry, and every consumer waiting on
   * this queue holds the same signal, so a caller that finds nothing goes back to waiting rather than
   * reporting empty.
   *
   * @param ns the queue being claimed from
   * @param demand what the caller asked for
   * @param asked when its call arrived, which is what the patience is measured from
   * @return the claim, or `None` when the patience elapsed; aborts with `QueueError` when the store fails
   */
  private def claimWithin(ns: Namespace, demand: Demand, asked: Instant): IO[QueueError, Option[Claimed]] =
    waiters.subscribe(demand.queue).flatMap { signal =>
      attempt(ns, demand).flatMap:
        case granted @ Some(_) => ZIO.succeed(granted)
        case None              =>
          remainingTime(demand.patience, asked).flatMap:
            case None       => ZIO.none
            case Some(left) =>
              signal.await(left).flatMap {
                case true  => claimWithin(ns, demand, asked)
                case false => ZIO.none
              }
    }

  /**
   * Claim whatever is claimable, without waiting.
   *
   * @param ns the queue to claim from
   * @param demand how much to take
   * @return the claim, or `None` when nothing was claimable; aborts with `QueueError` when the store fails
   */
  private def attempt(ns: Namespace, demand: Demand): IO[QueueError, Option[Claimed]] =
    connection.provide:
      scripts.consume.run(ns, leaseTtl, demand.batch)

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
      scripts.complete.run(Namespace(settlement.claimed.queue, buckets), settlement)

  /**
   * One `heartbeat` call '''per queue''', because claims are namespaced by queue while a caller's receipts
   * are not: a consumer holding work in three queues costs three round trips here.
   *
   * No worker entry is written, because there are none: a consumer is known by its receipts, and its
   * claims are found by fence token.
   *
   * @param claims every claim the caller still holds, across any number of queues
   * @return the new deadline and the claims already revoked
   */
  override def renew(claims: Chunk[Claim]): IO[QueueError, (Instant, Chunk[Claim])] =
    connection.provide:
      ZIO
        .foreach(claims.groupBy(_.queue).toList): (queue, held) =>
          scripts.heartbeat.run(Namespace(queue, buckets), leaseTtl, held)
        .map: results =>
          val (when, chunk) = results.unzip
          when.maxOption.getOrElse(Instant.EPOCH) -> Chunk.fromIterable(chunk).flatten

  /**
   * One `watchdog` call, which carries both sweeps — lapsed claims and elapsed backoffs — so
   * a repair pass is a single round trip and a single idle window on the server.
   *
   * @param queue the queue to repair
   * @param limit the most entries to handle in one pass
   * @return what it repaired
   */
  override def sweep(queue: QueueName, limit: Int): IO[QueueError, QueueStore.Swept] =
    connection.provide:
      scripts.watchdog.run(Namespace(queue, buckets), limit)


object RedisQueueStore:

  /**
   * The store.
   *
   * Nothing is started here any more: the store holds no registrations to renew, and the streams it waits
   * on is run by whoever owns the listener.
   *
   * @param connection where its connection comes from
   * @param scripts the loaded digests
   * @param waiters where a caller waits
   * @param buckets how many wake streams the deployment has, which decides every key's hash tag
   * @param leaseTtl how long a claim survives without a heartbeat
   * @return the store
   */
  def make(
    connection: Connection,
    scripts: Scripts,
    waiters: Waiters,
    leaseTtl: Duration,
    buckets: Int,
  ): UIO[RedisQueueStore] =
    ZIO.succeed(RedisQueueStore(connection, scripts, waiters, leaseTtl, buckets))
