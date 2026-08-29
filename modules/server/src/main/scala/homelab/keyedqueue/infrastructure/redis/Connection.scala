package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.types.WorkerId
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.codec.{ ByteArrayCodec, RedisCodec, StringCodec }
import io.lettuce.core.{ RedisClient, RedisURI }
import zio.*

import java.time.Duration as JavaDuration


/**
 * Where an effect gets a connection from.
 *
 * '''The caller declares whether it may block, and is given a connection accordingly.''' `BLMOVE` occupies
 * its connection for the whole wait, so an effect that parks must not run on one other callers are sharing —
 * and an effect that never parks should not have to wait for a free connection. Which of the two an operation
 * is, is known where it is written and nowhere else, so it is declared there rather than guessed at by
 * whatever holds the connections.
 *
 * The connection arrives in the environment as [[Connection.Commands]]: an effect asks for one by type, and
 * this decides which one it gets.
 */
trait Connection:

  /**
   * Run an effect that will not occupy its connection.
   *
   * @param effect what to run, needing a connection
   * @tparam R what it needs besides a connection
   * @tparam E how it fails
   * @tparam A what it produces
   * @return the same effect, its connection supplied
   */
  def provide[R, E, A](effect: ZIO[R & Connection.Commands, E, A]): ZIO[R, E, A]

  /**
   * Run an effect that may park its connection for as long as it likes.
   *
   * '''It is handed the connection's identity as well as the connection.''' A claim leaves its key in
   * `claiming:<worker>` between the `BLMOVE` and the script that finishes it, and only that worker's own
   * liveness can recover it — so the identity has to be the borrowed connection's, not the caller's.
   *
   * @param effect what to run, given the borrowed connection's identity and needing the connection itself
   * @tparam R what it needs besides a connection
   * @tparam E how it fails
   * @tparam A what it produces
   * @return the same effect, its connection supplied
   */
  def provideBlocking[R, E, A](effect: WorkerId => ZIO[R & Connection.Commands, E, A]): ZIO[R, E, A]


/**
 * One Redis connection, and the lifecycle around it.
 *
 * '''Keys are text, values are bytes.''' The scripts build key names by concatenation, so keys must be
 * exactly the UTF-8 the code wrote; payloads are opaque protobuf, so values must survive any byte. That is
 * what the mixed codec below buys, and it is why this adapter does not apply a client that encodes keys
 * through a schema codec.
 *
 * '''An idle connection is exclusive.''' `BLMOVE` occupies its connection for the whole wait, so a
 * claimer owns one outright rather than borrowing from a pool.
 */
object Connection:

  /**
   * A connection this object opened, and so one carrying the codec everything here assumes.
   *
   * Opaque so a connection cannot be conjured from any `RedisCommands`: the scripts depend on keys being the
   * exact UTF-8 they wrote and values passing through untouched, which is true of [[open]]'s codec and not
   * guaranteed of anything else. The `<:` keeps it usable as the Lettuce API at the apply site.
   */
  opaque type Commands <: RedisCommands[String, Array[Byte]] = RedisCommands[String, Array[Byte]]

  /**
   *
   * @param effect
   * @tparam R
   * @tparam E
   * @tparam A
   * @return
   */
  def use[R, E, A](effect: Commands => ZIO[R, E, A]): ZIO[R & Commands, E, A] =
    ZIO.serviceWithZIO[Connection.Commands](effect)

  /**
   * A connection reserved for claiming, and the identity that makes its death recoverable.
   *
   * The two travel together because `claiming:<worker>` is per worker: a key sits there between the
   * `BLMOVE` that took it and the script that finishes the claim, and the sweep that would recover it reads
   * the queue's worker set. Give two connections one identity and they share a claiming list — an
   * interrupted claim on one would hand back a key the other is still granting.
   *
   * @param commands the connection
   * @param worker what it announces itself as, unique to it and stable for its life
   */
  final case class Claiming(commands: Commands, worker: WorkerId)

  /**
   * One shared connection and `claimers` claiming connections, all closed with the scope.
   *
   * @param client the client to connect with
   * @param sharedTimeout the command ceiling for the shared connection
   * @param claimingTimeout the command ceiling for a claiming connection; must exceed the longest wait, or
   *                        Lettuce gives up on a `BLMOVE` that is doing exactly what it was asked to
   * @param claimers how many connections may be occupied at once
   * @return the pool; aborts with `QueueError` when a connection cannot be opened
   */
  def pool(
    client: RedisClient,
    sharedTimeout: Duration,
    claimingTimeout: Duration,
    claimers: Int,
  ): ZIO[Scope, QueueError, Connection] =
    for
      sync     <- open(client, sharedTimeout)
      claiming <- ZIO.foreach(Chunk.fromIterable(0 until claimers))(claim(client, claimingTimeout, _))
      idle     <- Queue.bounded[Claiming](claimers)
      _        <- idle.offerAll(claiming)
    yield Pool(sync, idle)

  /**
   * One claiming connection, with an identity of its own.
   *
   * @param client the client to connect with
   * @param commandTimeout the command ceiling
   * @param index distinguishes this connection from its peers within the process
   * @return the connection and its identity; aborts with `QueueError` when it cannot be opened
   */
  private def claim(client: RedisClient, commandTimeout: Duration, index: Int): ZIO[Scope, QueueError, Claiming] =
    open(client, commandTimeout).zipWith(identity(index))(Claiming.apply)

  /**
   * A worker id that is unique per connection and stable for its life.
   *
   * The host name makes it legible in `workers` when something is stuck; the random suffix keeps two pods
   * with the same name — or two runs of one pod — from colliding.
   *
   * @param index which claiming connection within this process
   * @return the id
   */
  private def identity(index: Int): UIO[WorkerId] =
    for
      host   <- System.env("HOSTNAME").orDie.map(_.getOrElse("local"))
      random <- Random.nextIntBounded(1 << 16)
    yield WorkerId(f"$host-$index-$random%04x")

  /** Keys as UTF-8 strings, values as raw bytes. */
  private val codec: RedisCodec[String, Array[Byte]] =
    RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE)

  /**
   * A client for the life of the scope.
   *
   * @param url the Redis URL, e.g. `redis://localhost:6379`
   * @return the client; aborts with `QueueError` when the URL is unusable
   */
  def client(url: String): ZIO[Scope, QueueError, RedisClient] =
    ZIO
      .acquireRelease {
        ZIO.attempt:
          val uri = RedisURI.create(url)
          RedisClient.create(uri)
      }(client => ZIO.attempt(client.shutdown()).ignore)
      .mapError(error => QueueError.StoreUnavailable(s"cannot reach $url: ${error.getMessage}"))

  /**
   * A connection for the life of the scope.
   *
   * The command timeout has to exceed the longest idle wait, or Lettuce gives up on a `BLMOVE` that is
   * doing exactly what it was asked to. It is set generously here and bounded in practice by the caller's
   * own deadline.
   *
   * @param client the client to connect with
   * @param commandTimeout the ceiling for any single command
   * @return the synchronous command API; aborts with `QueueError` when connecting fails
   */
  def open(client: RedisClient, commandTimeout: Duration): ZIO[Scope, QueueError, Connection.Commands] =
    ZIO
      .acquireRelease(
        ZIO.attemptBlocking(client.connect(codec))
      )(connection => ZIO.attemptBlocking(connection.close()).ignore)
      .mapAttempt: connection =>
        connection.setTimeout(JavaDuration.ofMillis(commandTimeout.toMillis))
        connection.sync()
      .mapError(error => QueueError.StoreUnavailable(s"cannot open a connection: ${error.getMessage}"))

  /**
   * One shared connection, and a fixed set of connections that may each be occupied by one caller at a time.
   *
   * '''The `idle` queue is the bound.''' A borrower takes a connection out of it and puts it back when the
   * effect finishes, so the queue's size is the ceiling on concurrent occupying calls, and a caller that
   * cannot get one waits for a peer rather than opening another. A `Queue` rather than a semaphore over an
   * array is what makes that waiting free: `take` parks the fiber until a connection is returned.
   *
   * @param sync the connection shared by everything that will not occupy it
   * @param idle the connections free to be handed out, one caller at a time, each with its identity
   */
  case class Pool(sync: Connection.Commands, idle: Queue[Connection.Claiming]) extends Connection:

    /**
     * Hand the effect the shared connection, without borrowing.
     *
     * Nothing is taken from `idle`, which is the point: an effect that will not occupy its connection must
     * never queue behind one that will, or a burst of claims parked in `BLMOVE` would stall work that needed
     * no connection of its own.
     *
     * @param effect what to run
     * @tparam R what it needs besides a connection
     * @tparam E how it fails
     * @tparam A what it produces
     * @return the same effect, its connection supplied
     */
    def provide[R, E, A](effect: ZIO[R & Connection.Commands, E, A]): ZIO[R, E, A] =
      effect.provideSomeEnvironment[R](env => env ++ ZEnvironment(sync))

    /**
     * Borrow a connection for the effect, and return it when the effect finishes.
     *
     * The borrow is an `acquireRelease` in its own scope, so the connection goes back on failure and on
     * interruption as much as on success — a connection lost here would shrink the pool for the life of the
     * process.
     *
     * @param effect what to run
     * @tparam R what it needs besides a connection
     * @tparam E how it fails
     * @tparam A what it produces
     * @return the same effect, its connection supplied
     */
    def provideBlocking[R, E, A](effect: WorkerId => ZIO[R & Connection.Commands, E, A]): ZIO[R, E, A] =
      ZIO.scoped(ZIO.acquireRelease(idle.take)(idle.offer).flatMap { borrowed =>
        effect(borrowed.worker).provideSomeEnvironment[R](env => env ++ ZEnvironment(borrowed.commands))
      })
