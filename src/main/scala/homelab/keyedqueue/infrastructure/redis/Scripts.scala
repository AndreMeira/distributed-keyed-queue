package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import io.lettuce.core.api.sync.RedisCommands
import zio.*

import java.nio.charset.StandardCharsets
import scala.io.Source


/**
 * The five Lua scripts, loaded once and called by digest.
 *
 * Loading up front rather than on first use keeps the failure at startup, where a missing or unparseable
 * script is obvious, instead of on the first message. There is no `NOSCRIPT` fallback for the same reason a
 * connection needs no reconnect logic here: a Redis restart takes the connection with it, and the process
 * re-registers when it reconnects.
 *
 * @param produce appends a message and makes its key claimable
 * @param consume turns possession of a key into a claim
 * @param complete settles the in-flight message and decides the key's next state
 * @param heartbeat renews worker liveness and the claims still held
 * @param watchdog the three repair sweeps
 */
final case class Scripts(
  produce: String,
  consume: String,
  complete: String,
  heartbeat: String,
  watchdog: String,
)


object Scripts:

  private val names = List("produce", "consume", "complete", "heartbeat", "watchdog")

  /**
   * Read the scripts from the classpath and register them with the server.
   *
   * @param redis the connection to register on
   * @return their digests; aborts with `QueueError` if one is missing or rejected
   */
  def load(redis: RedisCommands[String, Array[Byte]]): IO[QueueError, Scripts] =
    ZIO
      .foreach(names)(name => text(name).flatMap(register(redis, _)))
      .map:
        case List(produce, consume, complete, heartbeat, watchdog) =>
          Scripts(produce, consume, complete, heartbeat, watchdog)
        case other =>
          Scripts("", "", "", "", "") // unreachable: `names` is fixed and foreach preserves its length

  /**
   * Read one script from `resources/lua`.
   *
   * @param name the file name without its extension
   * @return the script text; aborts if it is missing from the jar
   */
  private def text(name: String): IO[QueueError, String] =
    ZIO
      .attempt(Source.fromResource(s"lua/$name.lua").mkString)
      .mapError(error => QueueError.MalformedReply(s"lua/$name.lua is missing: ${error.getMessage}"))

  /**
   * Register one script and keep its digest.
   *
   * @param redis the connection to register on
   * @param script the script text
   * @return the digest to call it by; aborts with `QueueError` if the store rejects it
   */
  private def register(redis: RedisCommands[String, Array[Byte]], script: String): IO[QueueError, String] =
    ZIO
      .attemptBlocking(redis.scriptLoad(script.getBytes(StandardCharsets.UTF_8)))
      .mapError(error => QueueError.StoreUnavailable(s"loading a script failed: ${error.getMessage}"))
