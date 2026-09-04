package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.sync.RedisClusterCommands
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
final case class Connection(sync: Connection.Commands, wake: Connection.Commands):

  /**
   * Run an effect on the shared connection.
   *
   * Everything except the doorbell answers immediately — a claim is one script, a settle is one script — so
   * one connection serves all of it.
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
   * Run an effect on the connection reserved for listening.
   *
   * '''One connection, one listener.''' A blocking `XREAD` owns its connection for the whole wait, so it
   * gets one of its own — sharing would put every claim and settle behind the doorbell. Nothing guards it,
   * because there is exactly one listener: a second caller would park behind the first one's block, which
   * is a bug in the caller rather than something to serialise here.
   *
   * @param effect what to run, needing a connection
   * @tparam R what it needs besides a connection
   * @tparam E how it fails
   * @tparam A what it produces
   * @return the same effect, its connection supplied
   */
  def listening[R, E, A](effect: ZIO[R & Connection.Commands, E, A]): ZIO[R, E, A] =
    effect.provideSomeEnvironment[R](env => env ++ ZEnvironment(wake))


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
   * The listening connection's command ceiling must exceed the longest block it will be asked to make, or
   * Lettuce abandons an `XREAD` that is doing exactly what it was told to. Derived here rather than passed
   * in, because nothing outside can get it right without knowing that.
   */
  private val listeningSlack: Duration = 10.seconds

  /** Keys as UTF-8 strings, values as raw bytes. */
  private val codec: RedisCodec[String, Array[Byte]] = RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE)

  /**
   * A connection this object opened, and so one carrying the codec everything here assumes.
   *
   * Opaque so a connection cannot be conjured from any commands object: the scripts depend on keys being the
   * exact UTF-8 they wrote and values passing through untouched, which is true of [[open]]'s codec and not
   * guaranteed of anything else. The `<:` keeps it usable as the Lettuce API at the use site.
   *
   * '''Bounded by `RedisClusterCommands`, which is the supertype of both backends.''' Lettuce has
   * `RedisCommands extends RedisClusterCommands`, and `RedisAdvancedClusterCommands` extends it too, so one
   * type covers a standalone server and a cluster. What the narrower bound would have added is
   * `RedisTransactionalCommands` — `MULTI` / `EXEC` — which this adapter must never use anyway: every
   * operation here is exactly one script, so that there are no interleavings to reason about. Losing it
   * from the type makes that a rule the compiler keeps rather than one the docs assert.
   */
  opaque type Commands <: RedisClusterCommands[String, Array[Byte]] = RedisClusterCommands[String, Array[Byte]]

  /**
   * Ask for the connection in the environment, and run something with it.
   *
   * '''The requiring side, not the providing one.''' A [[Connection]]'s `provide` and `provideBlocking`
   * decide which connection an effect gets; this is how the effect says it needs one at all. The two meet
   * in the environment: everything below the port — every script's `run`, and script registration — is
   * written as `ZIO[Commands, …]` and never learns whether the connection it was handed is shared,
   * borrowed, standalone or clustered.
   *
   * Takes a function rather than an effect because the Lettuce API is not effectful: the callers all wrap a
   * blocking call, and handing them the connection directly saves each one a `ZIO.service` of its own.
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
   * What the pool needs to reach Redis, and to size and time itself.
   *
   * @param maxWait the longest wait a caller may ask for; both command ceilings are derived from it
   * @param redisUrl where the substrate lives — one server, or a seed node of a cluster
   * @param cluster whether `redisUrl` names a cluster; Lettuce has no URL scheme that tells the two apart,
   *                so it is said here
   */
  final case class Config(maxWait: Duration, redisUrl: String, cluster: Boolean)

  /**
   * Two connections, closed with the scope: one shared by everything that answers immediately, and one
   * reserved for the doorbell. Named  for what it replaced; there is nothing to pool any more.
   *
   * Two rather than a pool because nothing else blocks any more — a claim is a single script that returns
   * at once, so the only long-lived wait in the process is the listener's `XREAD`.
   *
   * @param config where Redis is, and the longest wait to honour
   * @return the connections; aborts with `QueueError` when one cannot be opened
   */
  def make(config: Config): ZIO[Scope, QueueError, Connection] =
    for
      client <- client(config)
      sync   <- open(client, config.maxWait)
      wake   <- open(client, config.maxWait + listeningSlack)
    yield Connection(sync, wake)

  /**
   * The client the pool will open its connections from — the one place the two backends are chosen between.
   *
   * One per pool, not one per connection: a client owns Netty event loops and a timer wheel and is what
   * `shutdown` releases, while a connection is a socket taken from it. Creating one per connection would
   * multiply the threads by [[Config.claimers]] to no purpose.
   *
   * @param config where the substrate lives, and whether it is a cluster
   * @return the client, shut down with the scope; aborts with `QueueError` when the URL is unusable
   */
  private def client(config: Config): ZIO[Scope, QueueError, RedisClient | RedisClusterClient] =
    if config.cluster then redisClusterClient(config.redisUrl)
    else redisClient(config.redisUrl)

  /**
   * A client for the life of the scope.
   *
   * @param url the Redis URL, e.g. `redis://localhost:6379`
   * @return the client; aborts with `QueueError` when the URL is unusable
   */
  private def redisClient(url: String): ZIO[Scope, QueueError, RedisClient] =
    ZIO
      .acquireRelease {
        ZIO.attempt:
          val uri = RedisURI.create(url)
          RedisClient.create(uri)
      }(client => ZIO.attempt(client.shutdown()).ignore)
      .mapError(error => QueueError.StoreUnavailable(s"cannot reach $url: ${error.getMessage}"))

  /**
   * A cluster client for the life of the scope.
   *
   * The sibling of [[redisClient]], and the only place the two backends differ in setup: from here on a cluster
   * connection is a [[Commands]] like any other, because every key a script touches carries its queue's
   * `{q:<queue>}` hash tag and therefore lands in one slot.
   *
   * @param url a seed node, e.g. `redis://localhost:7000`
   * @return the client; aborts with `QueueError` when the URL is unusable
   */
  private def redisClusterClient(url: String): ZIO[Scope, QueueError, RedisClusterClient] =
    ZIO
      .acquireRelease {
        ZIO.attempt:
          val uri = RedisURI.create(url)
          RedisClusterClient.create(uri)
      }(client => ZIO.attempt(client.shutdown()).ignore)
      .mapError(error => QueueError.StoreUnavailable(s"cannot reach $url: ${error.getMessage}"))

  /**
   * A connection from whichever client [[redis]] returned.
   *
   * The union is what keeps the choice of backend out of the pool: from here on a connection is a
   * [[Commands]] whichever side it came from, because every key a script touches carries its queue's
   * `{q:<queue>}` hash tag and so lands in one slot either way.
   *
   * @param client the client to connect with
   * @param commandTimeout the ceiling for any single command
   * @return the synchronous command API; aborts with `QueueError` when connecting fails
   */
  private def open(client: RedisClient | RedisClusterClient, commandTimeout: Duration): ZIO[Scope, QueueError, Commands] =
    client match
      case c: RedisClient        => open(c, commandTimeout)
      case c: RedisClusterClient => open(c, commandTimeout)

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
  private def open(client: RedisClient, commandTimeout: Duration): ZIO[Scope, QueueError, Connection.Commands] =
    ZIO
      .acquireRelease(
        ZIO.attemptBlocking(client.connect(codec))
      )(connection => ZIO.attemptBlocking(connection.close()).ignore)
      .mapAttempt: connection =>
        connection.setTimeout(JavaDuration.ofMillis(commandTimeout.toMillis))
        connection.sync()
      .mapError(error => QueueError.StoreUnavailable(s"cannot open a connection: ${error.getMessage}"))

  /**
   * A cluster connection for the life of the scope.
   *
   * Identical in shape to the standalone [[open]]: the routing a cluster needs is Lettuce's business, and
   * the commands this returns satisfy the same [[Commands]] bound.
   *
   * @param client         the cluster client to connect with
   * @param commandTimeout the ceiling for any single command
   * @return the synchronous command API; aborts with `QueueError` when connecting fails
   */
  private def open(client: RedisClusterClient, commandTimeout: Duration): ZIO[Scope, QueueError, Connection.Commands] =
    ZIO
      .acquireRelease(
        ZIO.attemptBlocking(client.connect(codec))
      )(connection => ZIO.attemptBlocking(connection.close()).ignore)
      .mapAttempt: connection =>
        connection.setTimeout(JavaDuration.ofMillis(commandTimeout.toMillis))
        connection.sync()
      .mapError(error => QueueError.StoreUnavailable(s"cannot open a connection: ${error.getMessage}"))
