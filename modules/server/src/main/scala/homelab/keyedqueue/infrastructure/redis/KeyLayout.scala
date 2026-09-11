package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.infrastructure.configuration.Misconfigured
import homelab.keyedqueue.infrastructure.redis.script.LockKeys
import io.lettuce.core.{ ScanArgs, ScanCursor, SetArgs }
import zio.*

import java.nio.charset.StandardCharsets


/**
 * The store's record of the schema it was written under, and the boot-time check against it.
 *
 * The shape of everything this code stores — the structures, their encodings, and the bucket constant that
 * decides every key's hash tag — is a property of the code, while the keys outlive every instance. Running
 * new-shaped code against old-shaped data does not degrade, it fails in ways no error message will
 * explain: a structure read as the wrong type, a key looked for in a bucket it was never written to. This
 * records the schema version in the store itself, where the data is, and refuses to start an instance
 * whose code disagrees.
 */
object KeyLayout:

  /** Where the schema version is recorded. */
  private val schema: String = "dkq:layout:schema"

  /**
   * The shape of everything this code stores: the queue's and the lock's structures, the encodings written
   * into them, and [[Namespace.buckets]]. '''Bump it on any change an older instance would misread''' — a
   * structure changing type, a field changing meaning, an encoding changing form, the bucket constant
   * changing. That is a review discipline, not something the code can detect; an unbumped version makes
   * the check vouch for a compatibility that is not there.
   *
   * Gate-only, deliberately: a mismatch is refused, never migrated, and the remedy is always drain or
   * flush, then `layout accept`. What it does '''not''' cover is client-held state — a receipt format
   * change breaks holds the store never sees, and no store-side marker can catch it.
   */
  private val schemaVersion: Int = 1

  /**
   * Check this code's schema against the store's, recording it on a first boot.
   *
   * `SET NX` claims the marker atomically, so two first-boot instances racing with different versions
   * cannot both write: one records, the other reads what was recorded and refuses.
   *
   * @return noop; aborts with `Misconfigured` when the store was written under a different schema, and
   *         with `RedisFailure` when it cannot be reached
   */
  def verify: ZIO[Connection.Commands, ApplicationError, Unit] =
    claimed.flatMap:
      case None                                                 => ZIO.unit // first boot: this code's schema is now the store's
      case Some(recorded) if recorded == schemaVersion.toString => ZIO.unit
      case Some(recorded)                                       =>
        ZIO.fail:
          Misconfigured:
            s"this store was written with schema version $recorded, but this code expects $schemaVersion. " +
              "The stored structures have a different shape than this code expects, and running against " +
              "them fails in ways no error message will explain. If the store has been drained (or " +
              "flushed) on purpose, run the 'layout accept' mode once to record the new schema."

  /**
   * Overwrite the store's record with this code's schema — the `layout accept` run mode.
   *
   * The deliberate half of the ceremony the boot check enforces, and it '''verifies the drain it
   * requires''': any key under dkq's own prefixes means state written under the old schema, so accepting
   * over it is refused rather than trusted. Keys outside those prefixes are ignored — a Redis that already
   * existed is a supported home, and its other tenants are none of dkq's business. What accept cannot see
   * is instances: it must run with none attached, since a running instance re-checks nothing after boot.
   *
   * @return noop; aborts with `Misconfigured` when the store still holds keys, and with `RedisFailure`
   *         when it cannot be reached
   */
  def accept: ZIO[Connection.Commands, ApplicationError, Unit] =
    for
      _        <- drained
      previous <- recorded
      _        <- record
      _        <- ZIO.logInfo(
                    s"layout accepted: schema ${previous.getOrElse("unset")} -> $schemaVersion. " +
                      "Start instances only now: a running instance checks its layout at boot and never again."
                  )
    yield ()

  /** The prefixes dkq's keys live under: every queue bucket's tag, and the lock's. */
  private val footprint: Chunk[String] =
    Chunk.fromIterable(0 until Namespace.buckets).map(bucket => s"${Namespace.tag(bucket)}*")
      :+ s"${LockKeys.tag}*"

  /** The most leftover keys a refusal names, so the message stays a message. */
  private val examples: Int = 5

  /**
   * Refuse to proceed while the store holds any key under dkq's own prefixes.
   *
   * A scan over [[footprint]], not `DBSIZE`: the store may be shared — "we already have a Redis running"
   * is a supported way to deploy — so only dkq's keys are evidence of an undrained dkq, and everything
   * else in the store is deliberately not looked at.
   *
   * @return noop; aborts with `Misconfigured` naming some of the keys when any exist
   */
  private def drained: ZIO[Connection.Commands, ApplicationError, Unit] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(leftovers(redis))
        .mapError(error => RedisFailure.Unavailable(error.getMessage))
        .flatMap: found =>
          ZIO
            .fail(
              Misconfigured(
                s"the store still holds dkq keys (${found.mkString(", ")}${if found.size >= examples then ", …" else ""}), " +
                  "so dkq has not been drained. Accepting a schema over existing state strands it under " +
                  "the old one. Delete dkq's keys — or flush the store if it is dkq's alone — then run " +
                  "'layout accept' again."
              )
            )
            .when(found.nonEmpty)
            .unit

  /**
   * Some keys under dkq's prefixes, up to [[examples]] of them.
   *
   * Any hit is grounds to refuse; the rest are collected only to make the refusal concrete. Each prefix is
   * scanned to its end or until enough examples are in hand, whichever comes first.
   *
   * @param redis the connection
   * @return the keys found, possibly fewer than exist
   */
  private def leftovers(redis: Connection.Commands): Chunk[String] =
    footprint.foldLeft(Chunk.empty[String]): (found, pattern) =>
      if found.size >= examples then found else found ++ matching(redis, pattern, examples - found.size)

  /**
   * Keys matching one pattern, up to a cap.
   *
   * @param redis the connection
   * @param pattern the prefix pattern to scan for
   * @param cap the most keys to bring back
   * @return the keys found under the pattern, at most `cap`
   */
  private def matching(redis: Connection.Commands, pattern: String, cap: Int): Chunk[String] =
    val args                 = ScanArgs.Builder.matches(pattern).limit(500)
    var cursor               = redis.scan(args)
    var found: Chunk[String] = Chunk.fromIterable(cursor.getKeys.toArray.map(_.toString))
    while !cursor.isFinished && found.size < cap do
      cursor = redis.scan(ScanCursor.of(cursor.getCursor), args)
      found = found ++ Chunk.fromIterable(cursor.getKeys.toArray.map(_.toString))
    found.take(cap)

  /**
   * `SET NX` the marker, reading what is there when the claim loses.
   *
   * @return `None` when this call recorded it; the recorded text when someone already had
   */
  private def claimed: ZIO[Connection.Commands, RedisFailure, Option[String]] =
    attempt.flatMap:
      case Claim.Recorded    => ZIO.none
      case Claim.Found(text) => ZIO.some(text)
      // Lost the claim, then found nothing: the marker vanished between the two reads. Ask again.
      case Claim.Vanished    => claimed

  /**
   * One round of the claim: try to record, and read on losing.
   *
   * @return what happened
   */
  private def attempt: ZIO[Connection.Commands, RedisFailure, Claim] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking {
          if redis.set(schema, utf8(schemaVersion), SetArgs.Builder.nx()) == "OK" then Claim.Recorded
          else
            Option(redis.get(schema)) match
              case Some(bytes) => Claim.Found(String(bytes, StandardCharsets.UTF_8))
              case None        => Claim.Vanished
        }
        .mapError(error => RedisFailure.Unavailable(error.getMessage))

  /** What one round of claiming the marker can find. */
  private enum Claim:

    /** Nobody had recorded it; this call did. */
    case Recorded

    /** Someone recorded this before us. */
    case Found(text: String)

    /** The claim lost, yet the read found nothing — the marker was deleted between the two. */
    case Vanished

  /**
   * What the marker currently records.
   *
   * @return its text, absent when it was never written
   */
  private def recorded: ZIO[Connection.Commands, RedisFailure, Option[String]] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(Option(redis.get(schema)).map(bytes => String(bytes, StandardCharsets.UTF_8)))
        .mapError(error => RedisFailure.Unavailable(error.getMessage))

  /**
   * Write the marker, unconditionally.
   *
   * @return noop
   */
  private def record: ZIO[Connection.Commands, RedisFailure, Unit] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.set(schema, utf8(schemaVersion)))
        .mapError(error => RedisFailure.Unavailable(error.getMessage))
        .unit

  /**
   * A number as the marker stores it.
   *
   * @param value the number
   * @return its UTF-8 bytes
   */
  private def utf8(value: Int): Array[Byte] = value.toString.getBytes(StandardCharsets.UTF_8)
