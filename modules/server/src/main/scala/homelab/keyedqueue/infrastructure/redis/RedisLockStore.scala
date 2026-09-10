package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.lock.LockStore.Hold
import homelab.keyedqueue.domain.types.QueueName
import zio.*

import java.time.Instant


/**
 * The lock over Redis: three scripts, two structures, no sweep.
 *
 * '''Two structures, and no sweep.''' `held` (a zset of `name -> lease deadline`) and `fence` (a hash of
 * `name -> generation`) are all a lock needs — no `ready`, no `msgs`, nothing to order or carry. And
 * because [[LockScripts.acquire]] is named, it reclaims an expired lease inline, so a dead holder is
 * released by the next contender rather than a background pass. A lock holds no work, so a lock nobody
 * waits for needs no reclaiming — which is why there is no watchdog here.
 *
 * '''Blocking acquire reuses the queue's readiness path.''' [[acquire]] loops the named try behind
 * [[Readiness]] exactly as `RedisQueueStore.claim` loops `attempt`. A real release wakes a waiter on that
 * lock — keyed by name, so only its own waiters stir, and only when something was actually freed. The wake
 * is offered in-process here; across instances it would ride the same wake stream the queue uses (not built
 * in this sketch).
 *
 * @param connection where its connection comes from
 * @param scripts the loaded lock scripts
 * @param readiness where a waiter parks for a lock to be released
 */
final class RedisLockStore(
  connection: Connection,
  scripts: LockScripts,
  readiness: Readiness,
) extends LockStore:

  override def tryAcquire(name: String, ttl: Duration): IO[RedisFailure, Option[Hold]] =
    connection.provide:
      scripts.acquire.run(name, ttl)

  override def acquire(name: String, ttl: Duration, patience: Duration): IO[RedisFailure, Option[Hold]] =
    Clock.instant.flatMap(asked => acquireWithin(name, ttl, patience, asked))

  override def release(hold: Hold): IO[RedisFailure, Boolean] =
    connection
      .provide(scripts.release.run(hold.name, hold.token))
      // Wake a waiter parked on this lock, but only on a real release: a stale one freed nothing, so a wake
      // would send waiters to look at a lock still held. In-process here; a cross-instance build would
      // publish the wake to a shared channel instead, under the same condition.
      .tap(released => readiness.ready(QueueName(hold.name)).when(released))

  override def refresh(hold: Hold, ttl: Duration): IO[RedisFailure, (Instant, Boolean)] =
    connection.provide:
      scripts.refresh.run(hold.name, hold.token, ttl)

  /**
   * Wait for a release, retry the acquire, until it succeeds or the patience is spent — the lock's twin of
   * `RedisQueueStore.claimWithin`.
   *
   * @param name the lock to take
   * @param ttl the hold length to request
   * @param patience the total wait allowed
   * @param asked when the call arrived, which the patience is measured from
   * @return the hold, or `None` when the patience elapsed; aborts with `RedisFailure` when the store fails
   */
  private def acquireWithin(name: String, ttl: Duration, patience: Duration, asked: Instant): IO[RedisFailure, Option[Hold]] =
    remainingTime(patience, asked).flatMap:
      case None       => ZIO.none
      case Some(left) =>
        readiness
          .awaitReady(QueueName(name), left)(tryAcquire(name, ttl))
          .flatMap:
            case taken @ Some(_) => ZIO.succeed(taken)
            case None            => acquireWithin(name, ttl, patience, asked)

  /**
   * What is left of the caller's patience.
   *
   * @param patience what it was granted
   * @param asked when its call arrived
   * @return the time still to wait, or `None` when it is spent
   */
  private def remainingTime(patience: Duration, asked: Instant): UIO[Option[Duration]] =
    Clock.instant.map: now =>
      val left = patience.minus(Duration.fromInterval(asked, now))
      Option.when(left.toMillis > 0)(left)


object RedisLockStore:

  /**
   * Load the lock scripts and hand back the store.
   *
   * @param connection where its connection comes from
   * @param readiness where a waiter parks for a release
   * @return the store; aborts with `RedisFailure` when a script is missing or rejected
   */
  def make(connection: Connection, readiness: Readiness): ZIO[Connection.Commands, RedisFailure, RedisLockStore] =
    LockScripts.make.map(RedisLockStore(connection, _, readiness))
