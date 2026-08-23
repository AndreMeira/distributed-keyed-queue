package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ ClaimRef, Claimed }
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import io.lettuce.core.RedisClient
import zio.*

import java.time.Instant


/**
 * The claimers, and the rule that only they may block.
 *
 * A `BLMOVE` occupies its connection for the whole wait, so the number of connections is the ceiling on
 * concurrent `Dequeue` calls. Borrowing from a fixed pool makes that ceiling explicit — and makes a caller
 * that cannot get one wait for a peer rather than open connections without bound.
 *
 * '''Every claimer registers before it claims.''' Its first heartbeat is awaited during construction, so a
 * connection is known to the store before it can hold anything; one that claimed first would leave a
 * claiming list with no liveness entry to expire, and nothing could ever recover it.
 *
 * @param pool the idle claimers
 * @param all every claimer, for the heartbeat that keeps them registered
 */
final class ClaimerPool private (pool: Queue[QueueStore], all: Chunk[QueueStore]):

  /**
   * Borrow a claimer for one operation, and always give it back.
   *
   * @param use what to do with it
   * @tparam A what that produces
   * @return the result; aborts with whatever `use` aborts with
   */
  def borrow[A](use: QueueStore => IO[QueueError, A]): IO[QueueError, A] =
    ZIO.scoped(ZIO.acquireRelease(pool.take)(pool.offer(_)).flatMap(use))

  /**
   * Keep every claimer registered, whether or not it is holding anything.
   *
   * Registration lapses on silence, and a lapsed worker's in-transition keys are recovered by the sweep —
   * so an idle claimer must keep beating or a claim it makes later would be born unrecoverable.
   *
   * @return noop
   */
  def beat: UIO[Unit] = ZIO.foreachDiscard(all)(_.renew(Chunk.empty).ignore)


object ClaimerPool:

  /**
   * Open `config.claimers` connections, register each, and hold them for the life of the scope.
   *
   * @param client the Redis client to connect with
   * @param config how many, and with what lease
   * @return the pool; aborts with `QueueError` when a connection or a script cannot be set up
   */
  def make(client: RedisClient, config: QueueConfig): ZIO[Scope, QueueError, ClaimerPool] =
    for
      stores <- ZIO.foreach(Chunk.fromIterable(0 until config.claimers))(index => claimer(client, config, index))
      pool   <- Queue.bounded[QueueStore](config.claimers)
      _      <- pool.offerAll(stores)
    yield ClaimerPool(pool, stores)

  /**
   * One connection, its scripts, and its registration.
   *
   * @param client the Redis client
   * @param config the lease and timeout settings
   * @param index distinguishes this claimer from its peers within the process
   * @return the store bound to that connection
   */
  private def claimer(client: RedisClient, config: QueueConfig, index: Int): ZIO[Scope, QueueError, QueueStore] =
    for
      // The command timeout must exceed the longest blocking wait, or the client gives up on a BLMOVE that
      // is doing exactly what it was told to.
      redis   <- Connection.open(client, config.maxWait + 10.seconds)
      scripts <- Scripts.load(redis)
      id      <- identity(index)
      store    = RedisQueueStore(redis, scripts, id, config.leaseTtl)
      _       <- store.renew(Chunk.empty) // registration, awaited: never claim before being known
    yield store

  /**
   * A worker id that is unique per connection and stable for its life.
   *
   * The host name makes it legible in `workers` when something is stuck; the random suffix keeps two pods
   * with the same name — or two runs of one pod — from colliding.
   *
   * @param index which claimer within this process
   * @return the id
   */
  private def identity(index: Int): UIO[WorkerId] =
    for
      host   <- System.env("HOSTNAME").orDie.map(_.getOrElse("local"))
      random <- Random.nextIntBounded(1 << 16)
    yield WorkerId(f"$host-$index-$random%04x")
