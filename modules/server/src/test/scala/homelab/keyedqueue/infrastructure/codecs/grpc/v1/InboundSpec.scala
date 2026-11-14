package homelab.keyedqueue.infrastructure.codecs.grpc.v1


import com.google.protobuf.duration.Duration as ProtoDuration
import com.google.protobuf.timestamp.Timestamp
import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.infrastructure.codecs.grpc.v1.Inbound.toDomain
import homelab.keyedqueue.v1
import zio.*
import zio.test.*

import java.time.Instant


/**
 * What the codec refuses at the protocol boundary rather than converting: a `Duration` or a `Timestamp`
 * outside what protobuf allows.
 *
 * Converted blindly, one of them would read as another value (nanos past a second carried into the seconds),
 * or overflow and throw on the request path. A refusal is a `DecodingError`, which the gRPC layer answers with
 * `INVALID_ARGUMENT`; what is in range but not acceptable (a zero or negative wait) is validation's to refuse.
 */
object InboundSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Inbound")(
    test("a duration in range reads as itself") {
      assertTrue(waited(ProtoDuration(seconds = 1, nanos = 500_000_000)) == Right(1500.millis))
    },
    test("protobuf's bounds are read, not thrown on: the longest as infinite, the most negative as zero") {
      // ZIO's Duration counts nanoseconds in a Long, so anything past about 292 years is infinite to it, and a
      // negative span is zero. Validation then clamps the one and refuses the other.
      assertTrue(
        waited(ProtoDuration(seconds = 315_576_000_000L, nanos = 999_999_999)) == Right(Duration.Infinity),
        waited(ProtoDuration(seconds = -315_576_000_000L, nanos = -999_999_999)) == Right(Duration.Zero),
      )
    },
    test("a negative duration is well-formed, and reads as zero for validation to refuse") {
      assertTrue(waited(ProtoDuration(seconds = -1, nanos = -500_000_000)) == Right(Duration.Zero))
    },
    test("an absent duration reads as zero, for validation to refuse with the request's other problems") {
      assertTrue(v1.DequeueRequest("jobs", None).toDomain.map(_.maxWait) == Right(Duration.Zero))
    },
    test("nanos of a second or more are refused, not carried into the seconds") {
      assertTrue(
        refused(waited(ProtoDuration(seconds = 1, nanos = 1_000_000_000))),
        refused(waited(ProtoDuration(seconds = 0, nanos = -1_000_000_000))),
      )
    },
    test("seconds and nanos of opposite signs are refused, not summed") {
      assertTrue(
        refused(waited(ProtoDuration(seconds = 1, nanos = -1))),
        refused(waited(ProtoDuration(seconds = -1, nanos = 1))),
      )
    },
    test("seconds past protobuf's range are refused, including those that would overflow") {
      assertTrue(
        refused(waited(ProtoDuration(seconds = 315_576_000_001L))),
        refused(waited(ProtoDuration(seconds = -315_576_000_001L))),
        refused(waited(ProtoDuration(seconds = Long.MaxValue, nanos = 999_999_999))),
      )
    },
    test("every duration a request carries is checked: a settle's retry, a lock's ttl and wait") {
      val bad = Some(ProtoDuration(seconds = 1, nanos = -1))
      assertTrue(
        refused(v1.SettleRequest("receipt", retryAfter = bad).toDomain),
        refused(v1.ScheduleRequest("jobs", Some(Helper.protoMessage("k1", "m")), bad).toDomain),
        refused(v1.AcquireRequest("lock", bad, Some(ProtoDuration(seconds = 1))).toDomain),
        refused(v1.AcquireRequest("lock", Some(ProtoDuration(seconds = 1)), bad).toDomain),
        refused(v1.TryAcquireRequest("lock", bad).toDomain),
        refused(v1.RefreshRequest("receipt", bad).toDomain),
      )
    },
    test("a refusal names the field and what protobuf allows") {
      val reasons = v1.DequeueRequest("jobs", Some(ProtoDuration(seconds = 1, nanos = -1))).toDomain.left.map(_.message)
      assertTrue(reasons.left.exists(_.startsWith("maxWait: a duration needs seconds within")))
    },
    test("a timestamp in range reads as itself, its bounds included") {
      assertTrue(
        sent(Timestamp(seconds = 1, nanos = 5)) == Right(Some(Instant.ofEpochSecond(1, 5))),
        sent(Timestamp(seconds = -62_135_596_800L)).isRight,
        sent(Timestamp(seconds = 253_402_300_799L, nanos = 999_999_999)).isRight,
      )
    },
    test("a timestamp past protobuf's range, or with nanos outside a second, is refused rather than thrown on") {
      assertTrue(
        refused(sent(Timestamp(seconds = -62_135_596_801L))),
        refused(sent(Timestamp(seconds = 253_402_300_800L))),
        refused(sent(Timestamp(seconds = Long.MaxValue, nanos = 999_999_999))),
        refused(sent(Timestamp(seconds = 1, nanos = -1))),
        refused(sent(Timestamp(seconds = 1, nanos = 1_000_000_000))),
      )
    },
    test("a stored message with an unreadable sent_at is reported as unreadable") {
      val stored = Helper.protoMessage("k1", "m").copy(sentAt = Some(Timestamp(seconds = Long.MaxValue)))
      assertTrue(Inbound.message(stored).left.exists(_.contains("a timestamp needs seconds between")))
    },
  )

  private object Support {

    /**
     * A dequeue's wait, as the codec reads it.
     *
     * @param duration the `max_wait` the request carries
     * @return the wait, or the codec's refusal
     */
    def waited(duration: ProtoDuration): Either[Inbound.DecodingError, Duration] =
      v1.DequeueRequest("jobs", Some(duration)).toDomain.map(_.maxWait)

    /**
     * An enqueued message's send time, as the codec reads it.
     *
     * @param stamp the `sent_at` the message carries
     * @return the time, or the codec's refusal
     */
    def sent(stamp: Timestamp): Either[Inbound.DecodingError, Option[Instant]] =
      v1.EnqueueRequest("jobs", Some(Helper.protoMessage("k1", "m").copy(sentAt = Some(stamp)))).toDomain.map(_.message.sentAt)

    /**
     * Whether the codec refused a request.
     *
     * @param decoded what it made of the request
     * @tparam A the domain request
     * @return whether it is a `DecodingError`
     */
    def refused[A](decoded: Either[Inbound.DecodingError, A]): Boolean =
      decoded.isLeft
  }
