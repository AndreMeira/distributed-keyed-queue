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
 * right claim when it becomes so.
 */
object Layout:

  /** Where the queue's bucket count is recorded. */
  private val queueBuckets: String = "dkq:layout:queue-buckets"

  /** Where the lock's bucket count is recorded. */
  private val lockBuckets: String = "dkq:layout:lock-buckets"

  /** The lock's bucket count: fixed at one — a single `{dkq:locks}` slot — until bucketing lands. */
  private val lockBucketCount: Int = 1

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
    check(queueBuckets, "wake-buckets", config.wakeBuckets)
      *> check(lockBuckets, "the lock's bucket count", lockBucketCount)

  /**
   * Overwrite the store's record with this instance's layout — the `layout accept` run mode.
   *
   * The deliberate half of the ceremony the boot check enforces. It must only run against a drained store:
   * every key written under the old layout stays where it was, so accepting over live state is exactly the
   * breakage the check exists to prevent.
   *
   * @param config where this instance's bucket count comes from
   * @return noop; aborts with `RedisFailure` when the store cannot be reached
   */
  def accept(config: QueueConfig): ZIO[Connection.Commands, ApplicationError, Unit] =
    record(queueBuckets, config.wakeBuckets)
      *> record(lockBuckets, lockBucketCount)
      *> ZIO.logInfo(s"layout accepted: wake-buckets=${config.wakeBuckets}, lock buckets=$lockBucketCount")

  /**
   * Claim one marker, or compare against whoever claimed it first.
   *
   * @param key the marker
   * @param setting what to call the number in a refusal
   * @param configured this instance's value
   * @return noop; aborts with `Misconfigured` when the recorded value differs or cannot be read
   */
  private def check(key: String, setting: String, configured: Int): ZIO[Connection.Commands, ApplicationError, Unit] =
    claimed(key, configured).flatMap:
      case None                                              => ZIO.unit // first boot: this instance's layout is now the store's
      case Some(recorded) if recorded == configured.toString => ZIO.unit
      case Some(recorded)                                    =>
        ZIO.fail(
          Misconfigured(
            s"this store was written with $setting = $recorded, but this instance is configured with " +
              s"$configured. Changing it strands every key written under the old layout and breaks mutual " +
              s"exclusion on live state. If the store has been drained (or flushed) on purpose, run the " +
              s"'layout accept' mode once to record the new layout."
          )
        )

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
