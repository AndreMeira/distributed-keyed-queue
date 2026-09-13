package homelab.keyedqueue.infrastructure.redis.script


import homelab.keyedqueue.infrastructure.redis.script.LuaScript.{ Input, Output }
import homelab.keyedqueue.infrastructure.redis.{ Connection, RedisFailure }
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands
import zio.*

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.io.Source
import scala.jdk.CollectionConverters.*


/**
 * One registered Lua script: a digest, the shape of reply it was told to expect, and the types either end
 * of it speaks.
 *
 * '''A script is a value, not a class.''' What differs between scripts is what they are sent and what they
 * answer, and both are decided by `In` and `Out` through the encoder and decoder in scope — so a script
 * needs no code of its own, only the two types and the digest it was loaded under.
 *
 * `outputType` is an argument rather than something derived from `Out`, because the two are independent: a
 * `Boolean` arrives as an `INTEGER` reply and an `Option[…]` as a `MULTI` one. What the wire carries is the
 * script's business; what it means is the decoder's.
 *
 * @param sha the digest this script was registered under
 * @param outputType what the server is told the reply looks like
 * @tparam In what this script is sent
 * @tparam Out what it answers
 */
case class LuaScript[-In: Input.Encoder, +Out: Output.Decoder](
  sha: LuaScript.Sha,
  outputType: ScriptOutputType,
) {

  /**
   * Run the script and read its answer.
   *
   * @param input what to send it
   * @return what it answered; aborts with `Unavailable` when the store fails, and with `DecodingError`
   *         when the reply is not what the script promised
   */
  def execute(input: In): ZIO[Connection.Commands, RedisFailure, Out] =
    for
      in       = Input.Encoder[In].encode(input)
      result  <- runScript(in)
      decoded <- ZIO.fromEither(Output.Decoder[Out].decode(result))
    yield decoded

  /**
   * The call itself: `EVALSHA` on the connection, with whatever the store failed with lifted.
   *
   * @param input the keys and arguments, already encoded
   * @return the raw reply; aborts with `Unavailable` when the call fails
   */
  private def runScript(input: LuaScript.Input): ZIO[Connection.Commands, RedisFailure, LuaScript.Output] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking:
          redis.evalsha[Any](sha, outputType, input.key, input.args*)
        .map(LuaScript.Output.apply)
        .mapError(LuaScript.failure)

}


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

  /** Where a digest is made, once `SCRIPT LOAD` has answered with one. */
  object Sha:

    /**
     * A digest, trusted.
     *
     * @param value what `SCRIPT LOAD` returned
     * @return the digest
     */
    def apply(value: String): Sha = value

  /**
   * What a script is called with: the keys it may touch, and the arguments it reads.
   *
   * Two arrays and not one because Redis draws the line, not this code: KEYS is what the server routes and
   * a cluster checks for a single slot, ARGV is everything else. A key in the wrong one is a script that
   * works on a single server and is refused on a cluster.
   *
   * @param key the KEYS, in the order the script reads them
   * @param args the ARGV, in the order the script reads them
   */
  case class Input(key: Array[String], args: Array[Array[Byte]])

  /** Writing what a script is called with. */
  object Input:

    /**
     * How one type is written as a script's keys and arguments.
     *
     * @tparam A what is being sent
     */
    trait Encoder[-A]:

      /**
       * Write it.
       *
       * @param value what is being sent
       * @return the keys and arguments to call with
       */
      def encode(value: A): LuaScript.Input

    /** The encoders, and the conventions every script's arguments share. */
    object Encoder:

      /**
       * The encoder in scope for this type.
       *
       * @tparam A what is being sent
       * @return the encoder
       */
      def apply[A: Encoder as encoder]: Encoder[A] = encoder

      /**
       * Encode text the way the scripts expect to read it — every ARGV is bytes.
       *
       * @param value the text
       * @return its UTF-8 bytes
       */
      def utf8(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

      /**
       * Encode a delay the way every script reads one: millis, in the text of an integer.
       *
       * The counterpart of [[Output.Decoder.duration]] — stating the convention once on this side too, so
       * a script's arguments say *what* they carry rather than how to spell it.
       *
       * @param value the delay
       * @return its millis, as bytes
       */
      def millis(value: Duration): Array[Byte] = number(value.toMillis)

      /**
       * Encode a number the way the scripts read one: its text.
       *
       * @param value the number
       * @return its digits, as bytes
       */
      def number(value: Long): Array[Byte] = utf8(value.toString)

  /**
   * A reply, before anything has read it.
   *
   * Opaque over `Any` because that is honestly what Lettuce hands back for a script — a `java.lang.Long`,
   * an `Array[Byte]`, or a `java.util.List` of those, depending on what the Lua returned. Naming it keeps
   * that `Any` from spreading: only a decoder may look inside one.
   */
  opaque type Output <: Any = Any

  /** Reading what a script answered. */
  object Output:

    /**
     * A reply, as it arrived.
     *
     * @param value what Lettuce handed back
     * @return it, as something only a decoder may read
     */
    def apply(value: Any): Output = value

    /**
     * A description of what one script's reply looks like, and how to read it.
     *
     * @tparam A what the reply means once read
     */
    trait Decoder[+A]:

      /**
       * Read a reply.
       *
       * @param input the raw reply
       * @return its meaning, or `DecodingError` saying what was expected instead
       */
      def decode(input: LuaScript.Output): Decoder.Result[A]

    /**
     * Reading a reply into what it means.
     *
     * Lettuce hands back `Any` for a script — `java.lang.Long`, `Array[Byte]`, or a `java.util.List` of
     * those — because a Lua reply has no static shape. A decoder is a description of the shape one script
     * promised, built from the primitives below and combined with `map`, `flatMap` and `at`.
     *
     * Some of those primitives are conventions rather than wire types: a deadline, a delay and a count all
     * arrive as the same integer, and which one a reply means is the script's business, not Redis's.
     *
     * Decoders are pure. A reply that is not what the script promised is a value describing that, and
     * lifting it is the caller's business, at the point where it knows what failing means.
     */
    object Decoder:

      /**
       * What reading a reply answers: its meaning, or why it could not be read.
       *
       * @tparam A what the reply means once read
       */
      type Result[+A] = Either[RedisFailure.DecodingError, A]

      /**
       * The decoder in scope for this type.
       *
       * @tparam A what the reply means once read
       * @return the decoder
       */
      def apply[A: Decoder as decoder]: Decoder[A] = decoder

      /**
       * An integer reply.
       *
       * @return the decoder
       */
      def long: Decoder[Long] =
        case number: java.lang.Long => Right(number.longValue)
        case other                  => malformed("an integer", other)

      /**
       * An integer reply that fits in an `Int`.
       *
       * Redis counts come back 64-bit, and the ones this adapter reads as `Int` — a backlog, a delivery
       * count — are small by nature. One that does not fit is refused rather than truncated: a wrapped
       * count is worse than a refused reply nothing should have produced.
       *
       * @return the decoder
       */
      def int: Decoder[Int] = long.emap: value =>
        if value.isValidInt then Right(value.toInt)
        else Left(RedisFailure.DecodingError(s"expected an integer that fits in 32 bits, got $value"))

      /**
       * A deadline, as every script spells one: unix millis in an integer reply.
       *
       * The scripts take their own clock from Redis (`TIME`) and answer with absolute millis, so nothing
       * here has to agree with the caller's clock — only with that convention.
       *
       * @return the decoder
       */
      def instant: Decoder[Instant] = long.map(Instant.ofEpochMilli)

      /**
       * A delay, as every script spells one: millis in an integer reply.
       *
       * The counterpart of [[instant]], reading the same convention as a span rather than a moment. Which
       * of the two a reply means is the script's business — `lock/acquire.lua` answers a deadline when it
       * granted, and a delay until the answer can change when it queued.
       *
       * @return the decoder
       */
      def duration: Decoder[Duration] = long.map(Duration.fromMillis)

      /**
       * A bulk string reply, as the bytes it carries.
       *
       * @return the decoder
       */
      def bytes: Decoder[Chunk[Byte]] =
        case bytes: Array[Byte] => Right(Chunk.fromArray(bytes))
        case other              => malformed("a bulk string", other)

      /**
       * A bulk string reply, as text.
       *
       * @return the decoder
       */
      def text: Decoder[String] =
        bytes.map(chunk => LuaScript.text(chunk.toArray))

      /**
       * An array reply, with its elements still untyped.
       *
       * Rarely wanted on its own — [[Decoder.at]] and [[Decoder.many]] are how an array is usually read.
       *
       * @return the decoder
       */
      def list: Decoder[List[Any]] =
        case values: java.util.List[?] => Right(values.asScala.toList)
        case other                     => malformed("an array", other)

      /**
       * A decoder that reads nothing and answers with this.
       *
       * With [[Decoder.flatMap]], what makes a `for` comprehension able to check something the reply cannot
       * state for itself — a shape the script promises but the types do not.
       *
       * @param value the answer
       * @tparam A what it means
       * @return the decoder
       */
      def constant[A](value: A): Decoder[A] = _ => Right(value)

      /**
       * A decoder that reads nothing and refuses.
       *
       * The other half of [[constant]]: the branch a check takes when the reply is not what the script
       * promised, at a point where nothing is left to read.
       *
       * @param reason what was wrong with it
       * @return the decoder
       */
      def fail(reason: String): Decoder[Nothing] = _ => error(reason)

      /**
       * A collection the script promised would not be empty.
       *
       * An empty one means the script and this adapter disagree about a reply's shape: a script with
       * nothing to say answers with absence, not with an empty batch.
       *
       * @param values what was read out of the reply
       * @tparam A what one element means
       * @return the same values, known to be at least one; refuses when there are none
       */
      def nonEmpty[A](values: Chunk[A]): Decoder[NonEmptyChunk[A]] =
        NonEmptyChunk.fromChunk(values) match
          case Some(values) => constant(values)
          case None         => fail("expected at least one element, got none")

      /**
       * An array reply of exactly `arity` elements.
       *
       * A script returning the wrong number of elements is a script and an adapter that disagree, and
       * saying so once is better than the same disagreement surfacing as a missing element somewhere
       * further in.
       *
       * @param arity how many elements the script promises
       * @param decoder how to read the array once its size is known to be right
       * @tparam A what the reply means once read
       * @return the decoder
       */
      def sized[A](arity: Int)(decoder: Decoder[A]): Decoder[A] = input =>
        list.decode(input).flatMap { values =>
          if values.size == arity then decoder.decode(input)
          else error(s"expected $arity elements, got ${values.size}")
        }

      /**
       * Name a reply that was not what a script promised, without printing a payload into a log.
       *
       * @param value the unexpected reply
       * @return its type, or `nil`
       */
      def describe(value: Any): String =
        if value == null then "nil" else value.getClass.getName

      /**
       * Refuse a reply, as a result rather than a decoder.
       *
       * The half of [[fail]] that the combinators use, where a decoder is not what is wanted — inside
       * [[at]] and [[many]], which are already reading.
       *
       * @param reason what was wrong with it
       * @return the failure
       */
      def error(reason: String): Decoder.Result[Nothing] =
        Left(RedisFailure.DecodingError(reason))

      /**
       * A reply element that was not what the script promised.
       *
       * @param expected what the decoder was looking for
       * @param value what it found
       * @return the failure
       */
      private def malformed(expected: String, value: Any): Decoder.Result[Nothing] =
        error(s"expected $expected, got ${describe(value)}")

      extension [A](decoder: Decoder[A])

        /**
         * Turn what this reads into something else.
         *
         * @param f what to turn it into
         * @tparam B the result
         * @return the decoder
         */
        def map[B](f: A => B): Decoder[B] = input => decoder.decode(input).map(f)

        /**
         * Turn what this reads into something else that can itself fail.
         *
         * The bridge to anything already returning `Either[DecodingError, *]` — a stored message read back
         * out of its bytes, say.
         *
         * @param f what to turn it into, or why it could not be
         * @tparam B the result
         * @return the decoder
         */
        def emap[B](f: A => Either[RedisFailure.DecodingError, B]): Decoder[B] =
          input => decoder.decode(input).flatMap(f)

        /**
         * Read something else from the '''same''' reply, once this has been read.
         *
         * What makes a multi-element reply a `for` comprehension: each step reads its own position out of
         * the one reply, and the yield sees them all.
         *
         * @param f what to read next
         * @tparam B the result
         * @return the decoder
         */
        def flatMap[B](f: A => Decoder[B]): Decoder[B] =
          input => decoder.decode(input).flatMap(a => f(a).decode(input))

        /**
         * Read this out of one element of an array reply.
         *
         * @param index which element
         * @return the decoder, reading at that position
         */
        def at(index: Int): Decoder[A] = input =>
          list.decode(input).flatMap { values =>
            values.lift(index) match
              case Some(element) => decoder.decode(Output(element))
              case None          => error(s"expected at least ${index + 1} elements, got ${values.size}")
          }

        /**
         * Read this out of every element of an array reply.
         *
         * @return the decoder, in the order the elements came back
         */
        def many: Decoder[Chunk[A]] = input =>
          list.decode(input).flatMap { values =>
            values.foldLeft[Decoder.Result[Chunk[A]]](Right(Chunk.empty)): (soFar, element) =>
              soFar.flatMap(read => decoder.decode(Output(element)).map(read :+ _))
          }

        /**
         * Allow the reply to say "nothing", which Lua spells two ways.
         *
         * A script that returns `nil` and one that returns an empty array both arrive as absence, and
         * neither is a failure — it is the answer.
         *
         * @return the decoder
         */
        def orNone: Decoder[Option[A]] =
          case null                                                                   => Right(None)
          case values: java.util.List[?] if values.isEmpty                            => Right(None)
          // A script that answers `nil` inside a MULTI reply arrives as a one-element list holding null,
          // not as a null. Reading that as a decoding failure would turn "nothing to claim" — the
          // ordinary answer on an idle queue — into an error.
          case values: java.util.List[?] if values.size == 1 && values.get(0) == null => Right(None)
          case other                                                                  => decoder.decode(Output(other)).map(Some(_))

  /**
   * Read a script from `resources/lua` and register it with the server.
   *
   * Shared rather than repeated in each script's `load`, because the two failures it distinguishes are the
   * same for all of them: a file missing from the jar is a packaging fault, and a script the server rejects
   * is a syntax fault. What each script does own is its own name.
   *
   * Any connection will do: `SCRIPT LOAD` registers with the server, not with the caller, so the digest is
   * good on every connection to it.
   *
   * @param path the script's path on the classpath, e.g. `lua/queue/enqueue.lua`
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
      case standalone                                  => standalone.scriptLoad(script)

  /**
   * Read one script off the classpath.
   *
   * @param path the script's path on the classpath, e.g. `lua/queue/enqueue.lua`
   * @return the script text; aborts if it is missing from the jar
   */
  private def load(path: String): IO[RedisFailure, String] =
    ZIO
      .attempt(Source.fromResource(path).mkString)
      .mapError(error => RedisFailure.MalformedReply(s"$path is missing: ${error.getMessage}"))

  /**
   * Decode a bulk string.
   *
   * Total, and so not a [[Output.Decoder]]: the codec types values as `Array[Byte]`, so wherever the bytes are
   * already in hand there is nothing left that can fail. [[Output.Decoder.text]] is the version for a reply element
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
