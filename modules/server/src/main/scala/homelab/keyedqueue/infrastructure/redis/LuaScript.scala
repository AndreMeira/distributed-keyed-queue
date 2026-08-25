package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.sync.RedisCommands
import zio.*

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.*


/**
 * One call to one script: which digest, which keys, which arguments, and how to read the reply back.
 *
 * KEYS and ARGV are positional, and `evalsha` takes them as two flat arrays — so a call site that builds them
 * inline is a row of strings whose meaning is the order they are in, checked by nothing and readable only
 * against the `.lua` file. An implementation of this trait owns one script's positions and takes that
 * operation's real parameters instead, so the ordering is written down once, next to the script it belongs to.
 *
 * Implementations are built by [[Scripts]] rather than by hand: a script is called by a digest assigned when
 * it was loaded, so the one place holding those digests is the one place that can hand out a callable script.
 *
 * @tparam A what this script's reply means
 */
trait LuaScript[+A]:

  /**
   * The digest to call this script by, handed over by [[Scripts]] when it built this.
   *
   * @return the digest the script was loaded under
   */
  def sha: String

  /**
   * What shape of reply to expect, so Lettuce knows how to decode it.
   *
   * @return the output type matching what the script returns
   */
  def output: ScriptOutputType

  /**
   * The KEYS the script touches.
   *
   * @return them in the order the script reads them; position is the only thing naming them
   */
  def keys: Array[String]

  /**
   * The ARGV the script reads.
   *
   * @return them in the order the script reads them, encoded as the script expects
   */
  def args: Array[Array[Byte]]

  /**
   * Read a reply into what it means.
   *
   * @param value the raw reply
   * @return its meaning; aborts with `MalformedReply` when the reply is not what the script promised
   */
  def read(value: Any): IO[QueueError, A]


/**
 * Calling the scripts, and reading what they return.
 *
 * Lettuce hands back `Any` for a script — `java.lang.Long`, `Array[Byte]`, or a `java.util.List` of those —
 * because a LuaScript reply has no static shape. The untyped edge is confined here so the store and the claimer
 * read as the operations they are rather than as pattern matches over Java collections.
 */
object LuaScript:

  /**
   * Calling a script as something a connection does.
   *
   * Which connection a call runs on is the one thing an adapter here has to keep straight — claiming parks in
   * `BLMOVE` and must be on a borrowed connection, everything else must not be. As a method on the connection,
   * that answer is the subject of every call site rather than an argument among others.
   */
  object syntax:
    extension (redis: RedisCommands[String, Array[Byte]]) {

      /**
       * Call a script on this connection and read its reply.
       *
       * @param script what to call, built by [[Scripts]]
       * @tparam A what the script's reply means
       * @return the reply, read; aborts with `QueueError` when the store fails or the reply cannot be read
       */
      def execute[A](script: LuaScript[A]): IO[QueueError, A] =
        ZIO
          .attemptBlocking(redis.evalsha[Any](script.sha, script.output, script.keys, script.args*))
          .mapError(failure)
          .flatMap(script.read)
    }

  /**
   * A single integer reply.
   *
   * @param context what was called, for the error message
   * @param value the reply
   * @return the number, or `MalformedReply` when the reply was not an integer
   */
  def number(context: String)(value: Any): Either[QueueError, Long] = value match
    case number: java.lang.Long => Right(number.longValue)
    case other                  => Left(QueueError.MalformedReply(s"$context: expected an integer, got ${describe(other)}"))

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

  /**
   * Encode text the way the scripts expect to read it.
   *
   * @param value the text
   * @return its UTF-8 bytes
   */
  def utf8(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

  /**
   * Decode a bulk string.
   *
   * @param value the bytes a script returned
   * @return them as text
   */
  def text(value: Array[Byte]): String = String(value, StandardCharsets.UTF_8)

  /**
   * Name a reply that was not what a script promised, without printing a payload into a log.
   *
   * @param value the unexpected reply
   * @return its type, or `nil`
   */
  def describe(value: Any): String = if value == null then "nil" else value.getClass.getName

  /**
   * Wrap what Lettuce threw.
   *
   * Everything the substrate throws is transient by nature: the lease is the backstop.
   *
   * @param error what the call threw
   * @return it as a `StoreUnavailable`
   */
  def failure(error: Throwable): QueueError = QueueError.StoreUnavailable(error.getMessage)
