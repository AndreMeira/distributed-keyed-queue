package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.types.WorkerId
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import io.lettuce.core.RedisClient
import zio.*


/**
 * The claimers, and the rule that only they may block.
 *
 * A `BLMOVE` occupies its connection for the whole wait, so the number of connections is the ceiling on
 * concurrent claims. Borrowing from a fixed pool makes that ceiling explicit, and makes a caller that cannot
 * get one wait for a peer rather than open connections without bound.
 *
 * An adapter detail on purpose: [[RedisQueueStore]] owns one of these, so nothing above the port learns that
 * claiming is connection-bound.
 *
 * @param idle the claimers not currently in use
 * @param all every claimer, for the beat that keeps them registered
 */
final class ClaimerPool private (idle: Queue[Claimer], all: Chunk[Claimer]):

  /**
   * Borrow a claimer for one operation, and always give it back.
   *
   * @param use what to do with it
   * @tparam A what that produces
   * @return the result; aborts with whatever `use` aborts with
   */
  def borrow[A](use: Claimer => IO[QueueError, A]): IO[QueueError, A] =
    ZIO.scoped(ZIO.acquireRelease(idle.take)(idle.offer(_)).flatMap(use))

  /**
   * Keep every claimer registered, whether or not it is holding anything.
   *
   * @return noop
   */
  def beat: UIO[Unit] = ZIO.foreachDiscard(all)(_.beat)


object ClaimerPool:

  /**
   * Open `config.claimers` connections and hold them for the life of the scope.
   *
   * @param client the Redis client to connect with
   * @param config how many, and with what lease
   * @return the pool; aborts with `QueueError` when a connection or a script cannot be set up
   */
  def make(client: RedisClient, config: QueueConfig): ZIO[Scope, QueueError, ClaimerPool] =
    for
      claimers <- ZIO.foreach(Chunk.fromIterable(0 until config.claimers))(open(client, config, _))
      idle     <- Queue.bounded[Claimer](config.claimers)
      _        <- idle.offerAll(claimers)
    yield ClaimerPool(idle, claimers)

  /**
   * One connection, its scripts, and its identity.
   *
   * @param client the Redis client
   * @param config the lease and timeout settings
   * @param index distinguishes this claimer from its peers within the process
   * @return the claimer
   */
  private def open(client: RedisClient, config: QueueConfig, index: Int): ZIO[Scope, QueueError, Claimer] =
    for
      // The command timeout must exceed the longest blocking wait, or the client gives up on a BLMOVE that
      // is doing exactly what it was told to.
      redis   <- Connection.open(client, config.maxWait + 10.seconds)
      scripts <- Scripts.load(redis)
      id      <- identity(index)
      claimer <- Claimer.make(redis, scripts, id, config.leaseTtl)
    yield claimer

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
