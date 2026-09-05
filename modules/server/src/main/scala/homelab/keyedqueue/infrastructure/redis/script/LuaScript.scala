package homelab.keyedqueue.infrastructure.redis.script


import homelab.common.error.ApplicationError
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands
import zio.*

import java.nio.charset.StandardCharsets
import scala.io.Source
import scala.jdk.CollectionConverters.*


/**
 * Calling the scripts, and reading what they return.
 *
 * The untyped edge is confined here so the store reads as the operations it performs rather than as pattern
 * matches over Java collections.
 */
object LuaScript:

  /**
   * The digest a loaded script is called by.
   *
   * Named rather than left as `String` because every script's KEYS and ARGV are strings too: a digest in the
   * wrong position is the one mistake `evalsha` cannot catch, and it fails as the wrong script running
   * against another one's keys.
   */
  opaque type Sha <: String = String

  /**
   * A digest, trusted.
   *
   * @param value what `SCRIPT LOAD` returned
   * @return the digest
   */
  def Sha(value: String): Sha = value

  /**
   * Read a script from `resources/lua` and register it with the server.
   *
   * Shared rather than repeated in each script's `make`, because the two failures it distinguishes are the
   * same for all of them: a file missing from the jar is a packaging fault, and a script the server rejects
   * is a syntax fault. What each script does own is its own name.
   *
   * Any connection will do: `SCRIPT LOAD` registers with the server, not with the caller, so the digest is
   * good on every connection to it.
   *
   * @param path the script's path on the classpath, e.g. `lua/produce.lua`
   * @return the digest to call it by; aborts with `RedisFailure` if it is missing or rejected
   */
  def register(path: String): ZIO[Connection.Commands, RedisFailure, Sha] =
    load(path).flatMap: text =>
      Connection.use: redis =>
        ZIO
          .attemptBlocking(loadEverywhere(redis, text.getBytes(StandardCharsets.UTF_8)))
          .mapError(error => RedisFailure.Unavailable(s"loading a script failed: ${error.getMessage}"))
          .map(Sha.apply)

  /**
   * Register a script on every node that might be asked to run it.
   *
   * `RedisAdvancedClusterCommands` overrides the cluster-wide script commands — `SCRIPT FLUSH`, `SCRIPT
   * KILL` — but inherits `scriptLoad` unchanged, so on a cluster connection it still reaches a single node.
   * With no `NOSCRIPT` fallback anywhere (see [[Scripts]]), a call routed to any other node would simply
   * fail, which is why this reaches for the node-selection API instead.
   *
   * `upstream()` rather than `masters()`: the latter is the same selection under Lettuce's older name, and
   * is deprecated. Every node answers with the same digest, since a digest is a hash of the script — so
   * taking the first is not a choice between answers.
   *
   * @param redis the connection to register on
   * @param script the script's bytes
   * @return the digest the server gave it
   */
  private def loadEverywhere(redis: Connection.Commands, script: Array[Byte]): String =
    redis match
      case cluster: RedisAdvancedClusterCommands[?, ?] =>
        cluster.upstream().commands().scriptLoad(script).stream().findFirst().orElseThrow()
      case standalone                                  =>
        standalone.scriptLoad(script)

  /**
   * Read one script off the classpath.
   *
   * @param path the script's path on the classpath, e.g. `lua/produce.lua`
   * @return the script text; aborts if it is missing from the jar
   */
  private def load(path: String): IO[RedisFailure, String] =
    ZIO
      .attempt(Source.fromResource(path).mkString)
      .mapError(error => RedisFailure.MalformedReply(s"$path is missing: ${error.getMessage}"))

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
   * Total, and so not a [[Decode.Of]]: the codec types values as `Array[Byte]`, so wherever the bytes are
   * already in hand there is nothing left that can fail. [[Decode.text]] is the version for a reply element
   * that might not be bytes at all.
   *
   * @param value the bytes a script returned
   * @return them as text
   */
  def text(value: Array[Byte]): String = String(value, StandardCharsets.UTF_8)

  /**
   * Wrap what Lettuce threw.
   *
   * Everything the substrate throws is transient by nature: the lease is the backstop.
   *
   * @param error what the call threw
   * @return it as an `Unavailable`
   */
  def failure(error: Throwable): RedisFailure = RedisFailure.Unavailable(error.getMessage)

  /**
   * Reading a reply into what it means.
   *
   * Lettuce hands back `Any` for a script — `java.lang.Long`, `Array[Byte]`, or a `java.util.List` of those
   * — because a Lua reply has no static shape. A decoder is a description of the shape one script promised,
   * built from the four primitives below and combined with `map`, `flatMap` and `at`.
   *
   * '''Failures name where they happened.''' Every decoder carries the path it is reading, and [[Of.at]]
   * extends it — so a reply whose second element is the wrong type fails as `consume[1]: expected an
   * integer, got java.lang.String` rather than as one opaque complaint about the reply as a whole. Nothing
   * has to write those messages; they fall out of where the decoder was when it stopped.
   *
   * Decoders are pure. A reply that is not what the script promised is a value describing that, and lifting
   * it is the caller's business, at the point where it knows what failing means.
   */
  object Decode:

    /**
     * A description of what one script's reply looks like, and how to read it.
     *
     * @tparam A what the reply means once read
     */
    opaque type Of[+A] = (String, Any) => Either[RedisFailure, A]

    /**
     * An integer reply.
     *
     * @return the decoder
     */
    def long: Of[Long] = (path, value) =>
      value match
        case number: java.lang.Long => Right(number.longValue)
        case other                  => Left(malformed(path, "an integer", other))

    /**
     * A bulk string reply, as the bytes it carries.
     *
     * @return the decoder
     */
    def bytes: Of[Chunk[Byte]] = (path, value) =>
      value match
        case bytes: Array[Byte] => Right(Chunk.fromArray(bytes))
        case other              => Left(malformed(path, "a bulk string", other))

    /**
     * A bulk string reply, as text.
     *
     * @return the decoder
     */
    def text: Of[String] = bytes.map(chunk => LuaScript.text(chunk.toArray))

    /**
     * An array reply, with its elements still untyped.
     *
     * Rarely wanted on its own — [[Of.at]] and [[Of.each]] are how an array is usually read.
     *
     * @return the decoder
     */
    def list: Of[List[Any]] = (path, value) =>
      value match
        case values: java.util.List[?] => Right(values.asScala.toList)
        case other                     => Left(malformed(path, "an array", other))

    /**
     * A decoder that reads nothing and answers with this.
     *
     * With [[Of.flatMap]], what makes a `for` comprehension able to check something the reply cannot state
     * for itself — a shape the script promises but the types do not.
     *
     * @param value the answer
     * @tparam A what it means
     * @return the decoder
     */
    def succeed[A](value: A): Of[A] = (_, _) => Right(value)

    /**
     * A decoder that reads nothing and refuses.
     *
     * The other half of [[succeed]]: the branch a check takes when the reply is not what the script
     * promised, at a point where nothing is left to read.
     *
     * @param problem what was wrong with it
     * @return the decoder
     */
    def fail(problem: RedisFailure): Of[Nothing] = (_, _) => Left(problem)

    /**
     * An array reply of exactly `arity` elements.
     *
     * A script returning the wrong number of elements is a script and an adapter that disagree, and saying
     * so once is better than the same disagreement surfacing as a missing element somewhere further in.
     *
     * @param arity how many elements the script promises
     * @param decoder how to read the array once its size is known to be right
     * @tparam A what the reply means once read
     * @return the decoder
     */
    def sized[A](arity: Int)(decoder: Of[A]): Of[A] = (path, value) =>
      list(path, value).flatMap: values =>
        if values.size == arity then decoder(path, value)
        else Left(RedisFailure.MalformedReply(s"$path: expected $arity elements, got ${values.size}"))

    /**
     * Name a reply that was not what a script promised, without printing a payload into a log.
     *
     * @param value the unexpected reply
     * @return its type, or `nil`
     */
    def describe(value: Any): String = if value == null then "nil" else value.getClass.getName

    extension [A](decoder: Of[A])

      /**
       * Read a reply.
       *
       * @param context what was called, which every failure is named from
       * @param value the raw reply
       * @tparam A what the reply means once read
       * @return its meaning, or `MalformedReply` naming where reading stopped
       */
      def decode(context: String, value: Any): Either[RedisFailure, A] = decoder(context, value)

      /**
       * Turn what this reads into something else.
       *
       * @param f what to turn it into
       * @tparam B the result
       * @return the decoder
       */
      def map[B](f: A => B): Of[B] = (path, value) => decoder(path, value).map(f)

      /**
       * Turn what this reads into something else that can itself fail.
       *
       * The bridge to anything already returning `Either[RedisFailure, *]` — a stored message read back out of
       * its bytes, say.
       *
       * @param f what to turn it into, or why it could not be
       * @tparam B the result
       * @return the decoder
       */
      def emap[B](f: A => Either[RedisFailure, B]): Of[B] = (path, value) => decoder(path, value).flatMap(f)

      /**
       * Read something else from the '''same''' reply, once this has been read.
       *
       * What makes a multi-element reply a `for` comprehension: each step reads its own position out of the
       * one reply, and the yield sees them all.
       *
       * @param f what to read next
       * @tparam B the result
       * @return the decoder
       */
      def flatMap[B](f: A => Of[B]): Of[B] = (path, value) => decoder(path, value).flatMap(a => f(a)(path, value))

      /**
       * Read this out of one element of an array reply.
       *
       * @param index which element
       * @return the decoder, reading at that position and naming its failures for it
       */
      def at(index: Int): Of[A] = (path, value) =>
        list(path, value).flatMap: values =>
          values.lift(index) match
            case Some(element) => decoder(s"$path[$index]", element)
            case None          =>
              Left(RedisFailure.MalformedReply(s"$path: expected at least ${index + 1} elements, got ${values.size}"))

      /**
       * Read this out of every element of an array reply.
       *
       * @return the decoder, in the order the elements came back
       */
      def each: Of[Chunk[A]] = (path, value) =>
        list(path, value).flatMap: values =>
          values.zipWithIndex.foldLeft(Right(Chunk.empty): Either[RedisFailure, Chunk[A]]):
            case (soFar, (element, index)) =>
              soFar.flatMap(read => decoder(s"$path[$index]", element).map(read :+ _))

      /**
       * Allow the reply to say "nothing", which Lua spells two ways.
       *
       * A script that returns `nil` and one that returns an empty array both arrive as absence, and neither
       * is a failure — it is the answer.
       *
       * @return the decoder
       */
      def orNone: Of[Option[A]] = (path, value) =>
        value match
          case null                                                                   => Right(None)
          case values: java.util.List[?] if values.isEmpty                            => Right(None)
          // A script that answers `nil` inside a MULTI reply arrives as a one-element list holding null,
          // not as a null. Reading that as a malformed reply would turn "nothing to claim" — the ordinary
          // answer on an idle queue — into an error.
          case values: java.util.List[?] if values.size == 1 && values.get(0) == null => Right(None)
          case other                                                                  => decoder(path, other).map(Some(_))

    /**
     * A reply element that was not what the script promised.
     *
     * @param path where the decoder was when it stopped
     * @param expected what it was looking for
     * @param value what it found
     * @return the failure
     */
    private def malformed(path: String, expected: String, value: Any): RedisFailure =
      RedisFailure.MalformedReply(s"$path: expected $expected, got ${describe(value)}")
