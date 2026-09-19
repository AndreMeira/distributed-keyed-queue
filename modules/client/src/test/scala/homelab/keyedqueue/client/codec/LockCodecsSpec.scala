package homelab.keyedqueue.client.codec


import com.google.protobuf.duration.Duration as ProtoDuration
import com.google.protobuf.timestamp.Timestamp
import homelab.keyedqueue.client.LockError
import homelab.keyedqueue.client.lock.*
import homelab.keyedqueue.v1
import io.grpc.Status
import zio.*
import zio.test.*

import java.time.Instant


/**
 * Reading the wire's answers, where the wire can state more than the client can hold.
 *
 * A grant is a flag and four fields that mean something only when it is set, so the cases worth testing
 * are the ones where those disagree.
 */
object LockCodecsSpec extends ZIOSpecDefault:

  private val until  = Timestamp(1_700_000_000L, 500)
  private val span   = ProtoDuration(30L, 0)
  private val moment = Instant.ofEpochSecond(1_700_000_000L, 500)

  private def unreadable(error: LockError): Boolean =
    error match
      case LockError.Unreadable(_) => true
      case _                       => false

  def spec: Spec[TestEnvironment & Scope, Any] = suite("LockCodecs")(
    suite("acquired")(
      test("a grant carries the receipt, the fence, the deadline and the lease it was granted for") {
        val response = v1.AcquireResponse(acquired = true, "handle", 7L, Some(until), Some(span))
        LockCodecs
          .decode(response)
          .map: answer =>
            assertTrue(answer == Acquired.Granted(Hold(Receipt("handle"), Fence(7L), moment, 30.seconds)))
      },
      test("not acquired is an answer, and the other fields are not read") {
        val response = v1.AcquireResponse(acquired = false, "", 0L, None, None)
        assertTrue(LockCodecs.decode(response) == Right(Acquired.Unavailable))
      },
      test("a grant with no receipt is refused: nothing could release or renew it") {
        val response = v1.AcquireResponse(acquired = true, "", 7L, Some(until), Some(span))
        assertTrue(LockCodecs.decode(response).left.exists(unreadable))
      },
      test("a grant with no deadline is refused rather than carried inwards") {
        val response = v1.AcquireResponse(acquired = true, "handle", 7L, None, Some(span))
        assertTrue(LockCodecs.decode(response).left.exists(unreadable))
      },
      test("a grant with no lease span is refused: nothing could time its renewals") {
        val response = v1.AcquireResponse(acquired = true, "handle", 7L, Some(until), None)
        assertTrue(LockCodecs.decode(response).left.exists(unreadable))
      },
      test("a lease of no length is refused") {
        val response = v1.AcquireResponse(acquired = true, "handle", 7L, Some(until), Some(ProtoDuration(0L, 0)))
        assertTrue(LockCodecs.decode(response).left.exists(unreadable))
      },
      test("a deadline past the end of time is an error, not a thrown one") {
        // Nothing on the wire bounds these fields, so the decoder does: reading them is total, and an
        // answer this client cannot hold comes back as a value like every other refusal here.
        val far  = v1.AcquireResponse(acquired = true, "handle", 7L, Some(Timestamp(Long.MaxValue, 0)), Some(span))
        val odd  = v1.AcquireResponse(acquired = true, "handle", 7L, Some(Timestamp(0L, -1)), Some(span))
        val huge = v1.AcquireResponse(acquired = true, "handle", 7L, Some(until), Some(ProtoDuration(Long.MaxValue, 0)))
        assertTrue(
          LockCodecs.decode(far).left.exists(unreadable),
          LockCodecs.decode(odd).left.exists(unreadable),
          LockCodecs.decode(huge).left.exists(unreadable),
        )
      },
    ),
    suite("refreshed")(
      test("a renewal carries the new deadline and the lease it was granted for") {
        assertTrue(
          LockCodecs.decode(v1.RefreshResponse(renewed = true, Some(until), Some(span))) ==
            Right(Refreshed.Renewed(moment, 30.seconds))
        )
      },
      test("a lost hold is an answer") {
        assertTrue(LockCodecs.decode(v1.RefreshResponse(renewed = false, None, None)) == Right(Refreshed.Lost))
      },
      test("a renewal with no deadline is refused") {
        assertTrue(LockCodecs.decode(v1.RefreshResponse(renewed = true, None, Some(span))).isLeft)
      },
      test("a renewal with no lease span is refused") {
        assertTrue(LockCodecs.decode(v1.RefreshResponse(renewed = true, Some(until), None)).isLeft)
      },
    ),
    suite("failure")(
      test("a malformed request is the caller's to fix, and carries what was wrong") {
        val raised = Status.INVALID_ARGUMENT.withDescription("a lock name is required").asException()
        assertTrue(LockCodecs.failure(raised) == LockError.Rejected("a lock name is required"))
      },
      test("an unreachable deployment is worth retrying") {
        val unavailable = Status.UNAVAILABLE.asException()
        val timedOut    = Status.DEADLINE_EXCEEDED.asException()
        assertTrue(
          LockCodecs.failure(unavailable).isInstanceOf[LockError.Unreachable],
          LockCodecs.failure(timedOut).isInstanceOf[LockError.Unreachable],
        )
      },
      test("anything else is reported rather than interpreted") {
        val internal = Status.INTERNAL.asException()
        assertTrue(LockCodecs.failure(internal).isInstanceOf[LockError.Failed])
      },
    ),
  )
