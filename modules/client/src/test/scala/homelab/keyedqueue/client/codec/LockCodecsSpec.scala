package homelab.keyedqueue.client.codec


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
 * A grant is a flag and three fields that mean something only when it is set, so the cases worth testing
 * are the ones where those disagree.
 */
object LockCodecsSpec extends ZIOSpecDefault:

  private val until = Timestamp(1_700_000_000L, 500)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("LockCodecs")(
    suite("acquired")(
      test("a grant carries the receipt, the fence and the deadline") {
        val response = v1.AcquireResponse(acquired = true, "handle", 7L, Some(until))
        LockCodecs
          .decode(response)
          .map: answer =>
            assertTrue(
              answer == Acquired.Granted(
                Hold(Receipt("handle"), Fence(7L), Instant.ofEpochSecond(1_700_000_000L, 500))
              )
            )
      },
      test("not acquired is an answer, and the other fields are not read") {
        val response = v1.AcquireResponse(acquired = false, "", 0L, None)
        assertTrue(LockCodecs.decode(response) == Right(Acquired.Unavailable))
      },
      test("a grant with no deadline is refused rather than carried inwards") {
        val response = v1.AcquireResponse(acquired = true, "handle", 7L, None)
        assertTrue(LockCodecs.decode(response).left.exists {
          case LockError.Unreadable(_) => true
          case _                       => false
        })
      },
    ),
    suite("refreshed")(
      test("a renewal carries the new deadline") {
        assertTrue(
          LockCodecs.decode(v1.RefreshResponse(renewed = true, Some(until))) ==
            Right(Refreshed.Renewed(Instant.ofEpochSecond(1_700_000_000L, 500)))
        )
      },
      test("a lost hold is an answer") {
        assertTrue(LockCodecs.decode(v1.RefreshResponse(renewed = false, None)) == Right(Refreshed.Lost))
      },
      test("a renewal with no deadline is refused") {
        assertTrue(LockCodecs.decode(v1.RefreshResponse(renewed = true, None)).isLeft)
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
