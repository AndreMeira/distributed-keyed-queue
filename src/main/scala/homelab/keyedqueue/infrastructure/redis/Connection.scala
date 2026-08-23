package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.codec.{ ByteArrayCodec, RedisCodec, StringCodec }
import io.lettuce.core.{ RedisClient, RedisURI }
import zio.*

import java.time.Duration as JavaDuration


/**
 * One Redis connection, and the lifecycle around it.
 *
 * '''Keys are text, values are bytes.''' The scripts build key names by concatenation, so keys must be
 * exactly the UTF-8 the code wrote; payloads are opaque protobuf, so values must survive any byte. That is
 * what the mixed codec below buys, and it is why this adapter does not use a client that encodes keys
 * through a schema codec.
 *
 * '''A blocking connection is exclusive.''' `BLMOVE` occupies its connection for the whole wait, so a
 * claimer owns one outright rather than borrowing from a pool.
 */
object Connection:

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
      .acquireRelease(ZIO.attempt(RedisClient.create(RedisURI.create(url))))(client => ZIO.attempt(client.shutdown()).ignore)
      .mapError(error => QueueError.StoreUnavailable(s"cannot reach $url: ${error.getMessage}"))

  /**
   * A connection for the life of the scope.
   *
   * The command timeout has to exceed the longest blocking wait, or Lettuce gives up on a `BLMOVE` that is
   * doing exactly what it was asked to. It is set generously here and bounded in practice by the caller's
   * own deadline.
   *
   * @param client the client to connect with
   * @param commandTimeout the ceiling for any single command
   * @return the synchronous command API; aborts with `QueueError` when connecting fails
   */
  def open(client: RedisClient, commandTimeout: Duration): ZIO[Scope, QueueError, RedisCommands[String, Array[Byte]]] =
    ZIO
      .acquireRelease(ZIO.attemptBlocking(client.connect(codec)))(connection => ZIO.attemptBlocking(connection.close()).ignore)
      .mapAttempt: connection =>
        connection.setTimeout(JavaDuration.ofMillis(commandTimeout.toMillis))
        connection.sync()
      .mapError(error => QueueError.StoreUnavailable(s"cannot open a connection: ${error.getMessage}"))
