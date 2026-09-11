package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.sync.RedisClusterCommands
import io.lettuce.core.codec.{ ByteArrayCodec, RedisCodec, StringCodec }
import io.lettuce.core.{ RedisClient, RedisURI }
import zio.*

import java.time.Duration as JavaDuration


/**
 * Where an effect gets a connection from.
 *
 * The connection arrives in the environment as [[Connection.Commands]]: an effect asks for one by type, and
 * this decides which one it gets. [[sync]] is shared by everything that answers immediately; [[listening]]
 * opens a fresh one for a reader that blocks — one per caller, because a blocked read occupies its
 * connection whole — timed generously enough that a blocking `XREAD` doing its job is not a timeout.
 *
 * Why the client is synchronous, and what that costs — Redis executes a script in about four microseconds,
 * a blocking-pool hop costs a third of one, and a waiting consumer holds no thread at all — is measured in
 * `docs/architecture/redis-connections.md`.
 */
final case class Connection(sync: Connection.Commands, client: Connection.Client, listeningTimeout: Duration):

  /**
   * Whether this talks to a cluster — which bounds what one command may touch, and so how a reader across
   * several keys has to be split.
   *
   * @return true when the client is a cluster client
   */
  def clustered: Boolean = client match
    case _: RedisClusterClient => true
    case _: RedisClient        => false

  /**
   * A connection of this caller's own, closed with the scope — for a reader that blocks, since a blocked
   * read occupies its connection whole.
   *
   * @return the commands; aborts with `Unavailable` when the connection cannot be opened
   */
  def listening: ZIO[Scope, RedisFailure, Connection.Commands] = Connection.open(client, listeningTimeout)

  /**
   * Run an effect on the shared connection.
   *
   * @param effect what to run, needing a connection
   * @tparam R what it needs besides a connection
   * @tparam E how it fails
   * @tparam A what it produces
   * @return the same effect, its connection supplied
   */
  def provide[R, E, A](effect: ZIO[R & Connection.Commands, E, A]): ZIO[R, E, A] =
    effect.provideSomeEnvironment[R](env => env ++ ZEnvironment(sync))


/**
 * One Redis connection, and the lifecycle around it.
 */
object Connection:

  /**
   */
  private val listeningSlack: Duration = 10.seconds

  /** Keys as UTF-8 strings, values as raw bytes. */
  private val codec: RedisCodec[String, Array[Byte]] =
    RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE)

  /**
   * A connection this object opened, and so one carrying the codec everything here assumes.
   *
   * Opaque so a connection cannot be conjured from any commands object: the scripts depend on keys being the
   * exact UTF-8 they wrote and values passing through untouched, which is true of [[open]]'s codec and not
   * guaranteed of anything else. The `<:` keeps it usable as the Lettuce API at the use site.
   */
  /** The two clients this object knows how to open, named so a [[Connection]] can hold one. */
  type Client = RedisClient | RedisClusterClient

  opaque type Commands <: RedisClusterCommands[String, Array[Byte]] =
    RedisClusterCommands[String, Array[Byte]]

  /**
   * Ask for the connection in the environment, and run something with it.
   *
   * @param effect what to run with the connection
   * @tparam R what it needs besides the connection
   * @tparam E how it fails
   * @tparam A what it produces
   * @return the same effect, now declaring that it needs a connection
   */
  def use[R, E, A](effect: Commands => ZIO[R, E, A]): ZIO[R & Commands, E, A] =
    ZIO.serviceWithZIO[Connection.Commands](effect)

  /**
   * What is needed to reach Redis, and to time the connections.
   *
   * @param maxWait the longest wait a caller may ask for; both command ceilings are derived from it
   * @param redisUrl where the substrate lives — one server, or a seed node of a cluster
   * @param cluster whether `redisUrl` names a cluster; Lettuce has no URL scheme that tells the two apart,
   *                so it is said here
   */
  final case class Config(maxWait: Duration, redisUrl: String, cluster: Boolean)

  /**
   * The shared connection, closed with the scope, over a client kept for opening listening ones.
   *
   * Listening connections are opened on demand rather than fixed at one, because how many are needed is
   * the listener's business: one on a single server, one per slot group on a cluster.
   *
   * @param config where Redis is, and the longest wait to honour
   * @return the connection; aborts with `Unavailable` when it cannot be opened
   */
  def make(config: Config): ZIO[Scope, RedisFailure, Connection] =
    for
      client <- client(config)
      sync   <- open(client, config.maxWait)
    yield Connection(sync, client, config.maxWait + listeningSlack)

  /**
   * The client both connections are opened from — the one place the two backends are chosen between.
   *
   * @param config where the substrate lives, and whether it is a cluster
   * @return the client, shut down with the scope; aborts with `Unavailable` when the URL is unusable
   */
  private def client(config: Config): ZIO[Scope, RedisFailure, Client] =
    if config.cluster then redisClusterClient(config.redisUrl)
    else redisClient(config.redisUrl)

  /**
   * A client for the life of the scope.
   *
   * @param url the Redis URL, e.g. `redis://localhost:6379`
   * @return the client; aborts with `Unavailable` when the URL is unusable
   */
  private def redisClient(url: String): ZIO[Scope, RedisFailure, RedisClient] =
    ZIO
      .acquireRelease {
        ZIO.attempt:
          val uri = RedisURI.create(url)
          RedisClient.create(uri)
      }(client => ZIO.attempt(client.shutdown()).ignore)
      .mapError(error => RedisFailure.Unavailable(s"cannot reach $url: ${error.getMessage}"))

  /**
   * A cluster client for the life of the scope.
   *
   * The sibling of [[redisClient]], and the only place the two backends differ in setup: from here on a cluster
   * connection is a [[Commands]] like any other, because every key a script touches carries its queue's
   * `{q:<queue>}` hash tag and therefore lands in one slot.
   *
   * @param url a seed node, e.g. `redis://localhost:7000`
   * @return the client; aborts with `Unavailable` when the URL is unusable
   */
  private def redisClusterClient(url: String): ZIO[Scope, RedisFailure, RedisClusterClient] =
    ZIO
      .acquireRelease {
        ZIO.attempt:
          val uri = RedisURI.create(url)
          RedisClusterClient.create(uri)
      }(client => ZIO.attempt(client.shutdown()).ignore)
      .mapError(error => RedisFailure.Unavailable(s"cannot reach $url: ${error.getMessage}"))

  /**
   * A connection from whichever client [[redis]] returned.
   *
   * The union is what keeps the choice of backend in one place: from here on a connection is a
   * [[Commands]] whichever side it came from, because every key a script touches carries its queue's
   * `{q:<queue>}` hash tag and so lands in one slot either way.
   *
   * @param client the client to connect with
   * @param commandTimeout the ceiling for any single command
   * @return the synchronous command API; aborts with `Unavailable` when connecting fails
   */
  private def open(client: Client, commandTimeout: Duration): ZIO[Scope, RedisFailure, Commands] =
    client match
      case c: RedisClient        => open(c, commandTimeout)
      case c: RedisClusterClient => open(c, commandTimeout)

  /**
   * A connection for the life of the scope.
   *
   * The command timeout has to exceed the longest block it will be asked to make, or Lettuce gives up on an
   * `XREAD` that is doing exactly what it was asked to. It is set generously here; the shared connection
   * never blocks at all.
   *
   * @param client the client to connect with
   * @param commandTimeout the ceiling for any single command
   * @return the synchronous command API; aborts with `Unavailable` when connecting fails
   */
  private def open(client: RedisClient, commandTimeout: Duration): ZIO[Scope, RedisFailure, Connection.Commands] =
    ZIO
      .acquireRelease(
        ZIO.attemptBlocking(client.connect(codec))
      )(connection => ZIO.attemptBlocking(connection.close()).ignore)
      .mapAttempt: connection =>
        connection.setTimeout(JavaDuration.ofMillis(commandTimeout.toMillis))
        connection.sync()
      .mapError(error => RedisFailure.Unavailable(s"cannot open a connection: ${error.getMessage}"))

  /**
   * A cluster connection for the life of the scope.
   *
   * Identical in shape to the standalone [[open]]: the routing a cluster needs is Lettuce's business, and
   * the commands this returns satisfy the same [[Commands]] bound.
   *
   * @param client         the cluster client to connect with
   * @param commandTimeout the ceiling for any single command
   * @return the synchronous command API; aborts with `Unavailable` when connecting fails
   */
  private def open(client: RedisClusterClient, commandTimeout: Duration): ZIO[Scope, RedisFailure, Connection.Commands] =
    ZIO
      .acquireRelease(
        ZIO.attemptBlocking(client.connect(codec))
      )(connection => ZIO.attemptBlocking(connection.close()).ignore)
      .mapAttempt: connection =>
        connection.setTimeout(JavaDuration.ofMillis(commandTimeout.toMillis))
        connection.sync()
      .mapError(error => RedisFailure.Unavailable(s"cannot open a connection: ${error.getMessage}"))
