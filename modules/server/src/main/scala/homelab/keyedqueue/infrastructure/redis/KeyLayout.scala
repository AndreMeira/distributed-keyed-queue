package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.infrastructure.configuration.{ Misconfigured, QueueConfig }
import io.lettuce.core.SetArgs
import zio.*

import java.nio.charset.StandardCharsets


/**
 * The store's record of the layout it was written under, and the boot-time check against it.
 *
 * The bucket count decides which hash tag every key carries, and it is configured per instance while the
 * keys outlive every instance — so nothing ties a deployment's opinion of the layout to the layout the data
 * actually has. Running with the wrong count does not degrade, it breaks mutual exclusion: keys are looked
 * for in buckets they were never written to. This records the counts in the store itself, where the data
 * is, and refuses to start an instance that disagrees.
 *
 * The lock's count is recorded even though it is not yet configurable, so the store already carries the
 * right claim when it becomes so. The schema version rides the same mechanism: the shape of the stored
 * structures, gated exactly like the counts — see [[schemaVersion]] for what bumping it means.
 */
object KeyLayout:

  /** Where the queue's bucket count is recorded. */
  private val queueBuckets: String = "dkq:layout:queue-buckets"

  /** Where the lock's bucket count is recorded. */
  private val lockBuckets: String = "dkq:layout:lock-buckets"

  /** The lock's bucket count: fixed at one — a single `{dkq:locks}` slot — until bucketing lands. */
  private val lockBucketCount: Int = 1

  /** Where the schema version is recorded. */
  private val schema: String = "dkq:layout:schema"

  /**
   * The shape of everything this code stores: the queue's and the lock's structures, and the encodings
   * written into them. '''Bump it on any change an older instance would misread''' — a structure changing
   * type, a field changing meaning, an encoding changing form. That is a review discipline, not something
   * the code can detect; an unbumped version makes the check vouch for a compatibility that is not there.
   *
   * Gate-only, deliberately: a mismatch is refused, never migrated, and the remedy is always drain or
   * flush, then `layout accept`. What it does '''not''' cover is client-held state — a receipt format
   * change breaks holds the store never sees, and no store-side marker can catch it.
   */
  private val schemaVersion: Int = 1

  /** Every marker, for the drain check and the accept log. */
  private val markers: Chunk[String] = Chunk(queueBuckets, lockBuckets, schema)

  /**
   * Check this instance's layout against the store's, recording it on a first boot.
   *
   * `SET NX` claims the marker atomically, so two first-boot instances racing with different counts cannot
   * both write: one records, the other reads what was recorded and refuses.
   *
   * @param config where this instance's bucket count comes from
   * @return noop; aborts with `Misconfigured` when the store was written under a different layout, and with
   *         `RedisFailure` when it cannot be reached
   */
  def verify(config: QueueConfig): ZIO[Connection.Commands, ApplicationError, Unit] =
    check(
      queueBuckets,
      "wake-buckets",
      config.wakeBuckets,
      "Changing it strands every key written under the old layout and breaks mutual exclusion on live state.",
    )
      *> check(
        lockBuckets,
        "the lock's bucket count",
        lockBucketCount,
        "Changing it strands every key written under the old layout and breaks mutual exclusion on live state.",
      )
      *> check(
        schema,
        "schema version",
        schemaVersion,
        "The stored structures have a different shape than this code expects, and running against them fails in ways no error message will explain.",
      )

  /**
   * Overwrite the store's record with this instance's layout — the `layout accept` run mode.
   *
   * The deliberate half of the ceremony the boot check enforces, and it '''verifies the drain it
   * requires''': any key beyond the markers themselves means state written under the old layout, so
   * accepting over it is refused rather than trusted — recording a new layout over live keys is exactly
   * the breakage the boot check exists to prevent, and this mode must not be the official way to cause it.
   * What it cannot see is instances: it must run with none attached, since a running instance re-checks
   * nothing after boot.
   *
   * @param config where this instance's bucket count comes from
   * @return noop; aborts with `Misconfigured` when the store still holds keys, and with `RedisFailure`
   *         when it cannot be reached
   */
  def accept(config: QueueConfig): ZIO[Connection.Commands, ApplicationError, Unit] =
    for
      _        <- drained
      previous <- ZIO.foreach(markers)(recorded)
      _        <- record(queueBuckets, config.wakeBuckets)
      _        <- record(lockBuckets, lockBucketCount)
      _        <- record(schema, schemaVersion)
      _        <- ZIO.logInfo(
                    s"layout accepted: wake-buckets ${previous(0).getOrElse("unset")} -> ${config.wakeBuckets}, " +
                      s"lock buckets ${previous(1).getOrElse("unset")} -> $lockBucketCount, " +
                      s"schema ${previous(2).getOrElse("unset")} -> $schemaVersion. " +
                      "Start instances only now: a running instance checks its layout at boot and never again."
                  )
    yield ()

  /**
   * Refuse to proceed while the store holds anything beyond the layout markers.
   *
   * `DBSIZE` rather than a pattern scan, because the store is this service's own — one DKQ per service,
   * like a database — so '''any''' unaccounted key is state the ceremony required to be gone.
   *
   * @return noop; aborts with `Misconfigured` when other keys exist
   */
  private def drained: ZIO[Connection.Commands, ApplicationError, Unit] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking {
          val keys     = redis.dbsize()
          val recorded = markers.count(key => redis.exists(key) > 0)
          keys - recorded
        }
        .mapError(error => RedisFailure.Unavailable(error.getMessage))
        .flatMap: others =>
          ZIO
            .fail(
              Misconfigured(
                s"the store holds $others key(s) beyond the layout markers, so it has not been drained. " +
                  "Accepting a layout over existing state strands it under the old one. Drain the store " +
                  "(no queued work, no outstanding receipts or holds) or flush it, then run 'layout accept' again."
              )
            )
            .when(others > 0)
            .unit

  /**
   * What one marker currently records.
   *
   * @param key the marker
   * @return its text, absent when it was never written
   */
  private def recorded(key: String): ZIO[Connection.Commands, RedisFailure, Option[String]] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(Option(redis.get(key)).map(bytes => String(bytes, StandardCharsets.UTF_8)))
        .mapError(error => RedisFailure.Unavailable(error.getMessage))

  /**
   * Claim one marker, or compare against whoever claimed it first.
   *
   * @param key the marker
   * @param setting what to call the number in a refusal
   * @param configured this instance's value
   * @param consequence what running with the mismatch would break, for the refusal's message
   * @return noop; aborts with `Misconfigured` when the recorded value differs or cannot be read
   */
  private def check(
    key: String,
    setting: String,
    configured: Int,
    consequence: String,
  ): ZIO[Connection.Commands, ApplicationError, Unit] =
    claimed(key, configured).flatMap:
      case None                                              => ZIO.unit // first boot: this instance's layout is now the store's
      case Some(recorded) if recorded == configured.toString => ZIO.unit
      case Some(recorded)                                    =>
        ZIO.fail:
          Misconfigured:
            s"this store was written with $setting = $recorded, but this instance expects $configured. " +
              s"$consequence If the store has been drained (or flushed) on purpose, run the " +
              s"'layout accept' mode once to record the new layout."

  /**
   * `SET NX` the marker, reading what is there when the claim loses.
   *
   * @param key the marker
   * @param value this instance's value
   * @return `None` when this call recorded it; the recorded text when someone already had
   */
  private def claimed(key: String, value: Int): ZIO[Connection.Commands, RedisFailure, Option[String]] =
    attempt(key, value).flatMap:
      case Claim.Recorded    => ZIO.none
      case Claim.Found(text) => ZIO.some(text)
      // Lost the claim, then found nothing: the marker vanished between the two reads. Ask again.
      case Claim.Vanished    => claimed(key, value)

  /**
   * One round of the claim: try to record, and read on losing.
   *
   * @param key the marker
   * @param value this instance's value
   * @return what happened
   */
  private def attempt(key: String, value: Int): ZIO[Connection.Commands, RedisFailure, Claim] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking {
          if redis.set(key, utf8(value), SetArgs.Builder.nx()) == "OK" then Claim.Recorded
          else
            Option(redis.get(key)) match
              case Some(bytes) => Claim.Found(String(bytes, StandardCharsets.UTF_8))
              case None        => Claim.Vanished
        }
        .mapError(error => RedisFailure.Unavailable(error.getMessage))

  /** What one round of claiming a marker can find. */
  private enum Claim:

    /** Nobody had recorded it; this call did. */
    case Recorded

    /** Someone recorded this before us. */
    case Found(text: String)

    /** The claim lost, yet the read found nothing — the marker was deleted between the two. */
    case Vanished

  /**
   * Write one marker, unconditionally.
   *
   * @param key the marker
   * @param value the value to record
   * @return noop
   */
  private def record(key: String, value: Int): ZIO[Connection.Commands, RedisFailure, Unit] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.set(key, utf8(value)))
        .mapError(error => RedisFailure.Unavailable(error.getMessage))
        .unit

  /**
   * A number as the marker stores it.
   *
   * @param value the number
   * @return its UTF-8 text
   */
  private def utf8(value: Int): Array[Byte] = value.toString.getBytes(StandardCharsets.UTF_8)
