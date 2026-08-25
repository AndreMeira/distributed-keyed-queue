package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.sync.RedisCommands
import zio.*

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.*


/**
 * Calling the scripts, and reading what they return.
 *
 * Lettuce hands back `Any` for a script — `java.lang.Long`, `Array[Byte]`, or a `java.util.List` of those —
 * because a Lua reply has no static shape. The untyped edge is confined here so the store and the claimer
 * read as the operations they are rather than as pattern matches over Java collections.
 */
object Lua:

  /**
   * Call a loaded script.
   *
   * @param redis the connection to call on
   * @param sha the digest from [[Scripts]]
   * @param output what shape of reply to expect
   * @param keys the KEYS, in the order the script reads them
   * @param args the ARGV
   * @return the raw reply; aborts with `QueueError` when the store fails
   */
  def call(
    redis: RedisCommands[String, Array[Byte]],
    sha: String,
    output: ScriptOutputType,
    keys: Array[String],
    args: Array[Array[Byte]],
  ): IO[QueueError, Any] =
    ZIO.attemptBlocking(redis.evalsha[Any](sha, output, keys, args*)).mapError(failure)

  /**
   * A single integer reply.
   *
   * @param context what was called, for the error message
   * @param value the reply
   * @return the number; aborts when the reply was not an integer
   */
  def number(context: String)(value: Any): IO[QueueError, Long] = value match
    case number: java.lang.Long => ZIO.succeed(number.longValue)
    case other                  => ZIO.fail(QueueError.MalformedReply(s"$context: expected an integer, got ${describe(other)}"))

  /**
   * The strings in a nested array reply, ignoring anything that is not one.
   *
   * @param value the reply element
   * @return its strings, in order
   */
  def strings(value: Any): Chunk[String] = value match
    case values: java.util.List[?] =>
      Chunk.fromIterable(values.asScala.toList).collect { case bytes: Array[Byte] => text(bytes) }
    case _                         => Chunk.empty

  /** Encode text the way the scripts expect to read it. */
  def utf8(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

  /** Decode a bulk string. */
  def text(value: Array[Byte]): String = String(value, StandardCharsets.UTF_8)

  /** Name a reply that was not what a script promised, without printing a payload into a log. */
  def describe(value: Any): String = if value == null then "nil" else value.getClass.getName

  /** Everything the substrate throws is transient by nature: the lease is the backstop. */
  def failure(error: Throwable): QueueError = QueueError.StoreUnavailable(error.getMessage)
