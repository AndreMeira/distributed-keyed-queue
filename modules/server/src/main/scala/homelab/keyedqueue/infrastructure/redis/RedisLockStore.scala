package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.service.lock.LockStore.Hold
import homelab.keyedqueue.domain.types.QueueName
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import io.lettuce.core.ScriptOutputType
import zio.*

import java.time.Instant


/**
 * The lock over Redis: three scripts, two structures, no sweep.
 *
 * '''Two structures, `held` and `fence`, both in one slot.''' `held` is a zset of `name -> lease deadline`,
 * `fence` a hash of `name -> generation`. Both keys carry the same hash tag so a script may touch both,
 * exactly as the queue's scripts share a tag. There is no `ready`, no `msgs`, no `owned` — a lock has
 * nothing to order and nothing to carry.
 *
 * '''Blocking acquire reuses the queue's readiness path.''' A named `tryAcquire` script does the atomic
 * work; [[acquire]] loops it behind [[Readiness]] exactly as `RedisQueueStore.claim` loops `attempt`, so a
 * waiter parks until a release wakes it or its patience runs out. In this sketch the wake is offered
 * in-process on release; across instances it would ride the same wake stream the queue uses, the release
 * appending an entry that names the lock (not built here).
 *
 * '''No sweep.''' `tryAcquire` reclaims an expired lease inline, so a dead holder is released by the next
 * contender rather than a background pass — see [[script.LuaScript]] callers for the queue's sweep, which a
 * lock does not need because it holds no work.
 *
 * @param connection where its connection comes from
 * @param acquireSha the loaded `lock_acquire.lua`
 * @param releaseSha the loaded `lock_release.lua`
 * @param refreshSha the loaded `lock_refresh.lua`
 * @param readiness where a waiter parks for a lock to be released
 */
final class RedisLockStore(
  connection: Connection,
  acquireSha: LuaScript.Sha,
  releaseSha: LuaScript.Sha,
  refreshSha: LuaScript.Sha,
  readiness: Readiness,
) extends LockStore:

  import RedisLockStore.*

  override def tryAcquire(name: String, ttl: Duration): IO[RedisFailure, Option[Hold]] =
    connection.provide:
      Connection.use: redis =>
        ZIO
          .attemptBlocking(redis.evalsha[Any](acquireSha, ScriptOutputType.MULTI, keys(name), utf8(name), millis(ttl)))
          .mapError(LuaScript.failure)
          .flatMap(reply => ZIO.fromEither(readHold(name)(reply)))

  override def acquire(name: String, ttl: Duration, patience: Duration): IO[RedisFailure, Option[Hold]] =
    Clock.instant.flatMap(asked => acquireWithin(name, ttl, patience, asked))

  override def release(hold: Hold): IO[RedisFailure, Boolean] =
    connection
      .provide:
        Connection.use: redis =>
          ZIO
            .attemptBlocking(redis.evalsha[Any](releaseSha, ScriptOutputType.INTEGER, keys(hold.name), utf8(hold.name), utf8(hold.token.toString)))
            .mapError(LuaScript.failure)
            .flatMap(reply => ZIO.fromEither(LuaScript.Decode.long.map(_ == 1L).decode("lock.release", reply)))
      // Wake a waiter parked on this lock. In-process here; a cross-instance build appends a wake entry.
      .tap(_ => readiness.ready(QueueName(hold.name)))

  override def refresh(hold: Hold, ttl: Duration): IO[RedisFailure, (Instant, Boolean)] =
    connection.provide:
      Connection.use: redis =>
        ZIO
          .attemptBlocking(
            redis.evalsha[Any](refreshSha, ScriptOutputType.MULTI, keys(hold.name), utf8(hold.name), utf8(hold.token.toString), millis(ttl))
          )
          .mapError(LuaScript.failure)
          .flatMap(reply => ZIO.fromEither(readRefresh(reply)))

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

  /** The hash tag every lock key shares — one slot, as the queue's keys share theirs. */
  private val tag: String = "{dkq:locks}"

  private def keys(name: String): Array[String] = Array(s"$tag:held", s"$tag:fence")

  private def utf8(value: String): Array[Byte]        = LuaScript.utf8(value)
  private def millis(duration: Duration): Array[Byte] = LuaScript.utf8(duration.toMillis.toString)

  /**
   * Read an acquire reply: `{token, leaseUntil}` when taken, nil when held.
   *
   * @param name the lock the reply is for
   * @param value the raw reply
   * @return the hold, or `None`; `MalformedReply` when the reply is neither
   */
  private def readHold(name: String)(value: Any): Either[RedisFailure, Option[Hold]] =
    LuaScript.Decode
      .sized(2) {
        for
          token <- LuaScript.Decode.long.at(0)
          until <- LuaScript.Decode.long.at(1)
        yield Hold(name, token, Instant.ofEpochMilli(until))
      }
      .orNone
      .decode("lock.acquire", value)

  /**
   * Read a refresh reply: `{leaseUntil, ok}`.
   *
   * @param value the raw reply
   * @return the new deadline and whether the hold survived
   */
  private def readRefresh(value: Any): Either[RedisFailure, (Instant, Boolean)] =
    LuaScript.Decode
      .sized(2) {
        for
          until <- LuaScript.Decode.long.at(0)
          ok    <- LuaScript.Decode.long.at(1)
        yield (Instant.ofEpochMilli(until), ok == 1L)
      }
      .decode("lock.refresh", value)

  /**
   * Load the three lock scripts and hand back the store.
   *
   * @param connection where its connection comes from
   * @param readiness where a waiter parks for a release
   * @return the store; aborts with `RedisFailure` when a script is missing or rejected
   */
  def make(connection: Connection, readiness: Readiness): ZIO[Connection.Commands, RedisFailure, RedisLockStore] =
    for
      acquire <- LuaScript.register("lua/lock_acquire.lua")
      release <- LuaScript.register("lua/lock_release.lua")
      refresh <- LuaScript.register("lua/lock_refresh.lua")
    yield RedisLockStore(connection, acquire, release, refresh, readiness)
