package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.infrastructure.redis.keys.{ KeyLayout, RedisKey }
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
 * this decides which one it gets. [[sync]] is shared by everything that answers immediately; [[blocking]]
 * holds one connection per partition — separate because a blocking command occupies its connection whole,
 * and one each because a partition's keys are a slot of their own, which a command may not leave. All of
 * them are opened at startup, so how many connections a deployment holds is a fact fixed before it serves.
 *
 * '''What each connection is for is not recorded here.''' Which keys a blocking read names is the reader's
 * to decide; this says only which connection serves a partition.
 *
 * Why the client is synchronous, and what that costs — Redis executes a script in about four microseconds,
 * a blocking-pool hop costs a third of one, and a waiting consumer holds no thread at all — is measured in
 * `docs/architecture/redis-connections.md`.
 *
 * @param sync the connection every command that answers at once runs on
 * @param blocking partition -> the connection reserved for commands that park, one per partition
 */
final case class Connection(sync: Connection.Commands, blocking: Map[KeyLayout.Partition, Connection.Commands]):

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
   * Run an effect on the connection reserved for a partition — the one a command may occupy by blocking.
   *
   * @param partition whose connection to run on
   * @param effect what to run, needing a connection
   * @tparam R what it needs besides a connection
   * @tparam E how it fails
   * @tparam A what it produces
   * @return the same effect, its connection supplied; aborts with `PartitionConnectionMissing` when no
   *         connection was opened for that partition, which is this code wired wrong rather than Redis
   *         being unreachable
   */
  def provideBlocking[R, E, A](
    partition: KeyLayout.Partition
  )(
    effect: ZIO[R & Connection.Commands, E, A]
  ): ZIO[R, E | RedisFailure, A] =
    blocking.get(partition) match
      case Some(connection) => effect.provideSomeEnvironment[R](env => env ++ ZEnvironment(connection))
      case None             => ZIO.fail(RedisFailure.PartitionConnectionMissing(partition))


/**
 * One Redis connection, and the lifecycle around it.
 */
object Connection:

  /**
   * How much longer than the longest wait a blocking connection's command timeout runs.
   *
   * A command timeout shorter than the block it is asked to make turns a read doing exactly what it was
   * told into a timeout, so the ceiling has to clear it — see `redis-streams-with-lettuce.md`.
   */
  private val listeningSlack: Duration = 10.seconds

  /** Keys as UTF-8 strings, values as raw bytes. */
  private val codec: RedisCodec[String, Array[Byte]] =
    RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE)

  /** The two clients this object knows how to open. */
  private type Client = RedisClient | RedisClusterClient

  /**
   * A connection this object opened, and so one carrying the codec everything here assumes.
   *
   * Opaque so a connection cannot be conjured from any commands object: the scripts depend on keys being the
   * exact UTF-8 they wrote and values passing through untouched, which is true of [[open]]'s codec and not
   * guaranteed of anything else. The `<:` keeps it usable as the Lettuce API at the use site.
   */
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
   * Every connection the deployment will hold, opened here and closed with the scope: the shared one, and
   * one per group that a single command may name.
   *
   * The count is settled at startup rather than left to the caller, so `CLIENT LIST` on a running store
   * shows what this says it will: one plus one per partition.
   *
   * @param config where Redis is, and the longest wait to honour
   * @param layout how many partitions there are, and so how many blocking connections to open
   * @return the connections; aborts with `Unavailable` when one cannot be opened
   */
  def make(config: Config, layout: KeyLayout): ZIO[Scope, RedisFailure, Connection] =
    for
      client   <- client(config)
      sync     <- open(client, config.maxWait)
      timeout   = config.maxWait + listeningSlack
      blocking <- ZIO.foreach(layout.partitionIds)(group => open(client, timeout).map(group -> _))
    yield Connection(sync, blocking.toMap)

  /**
   * The client every connection is opened from — the one place the two backends are chosen between.
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
   * connection is a [[Commands]] like any other, because every key a script touches carries its partition's
   * `{p:<partition>}` hash tag and therefore lands in one slot.
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
   * A connection from whichever client [[client]] returned.
   *
   * The union is what keeps the choice of backend in one place: from here on a connection is a
   * [[Commands]] whichever side it came from, because every key a script touches carries its partition's
   * `{p:<partition>}` hash tag and so lands in one slot either way.
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
