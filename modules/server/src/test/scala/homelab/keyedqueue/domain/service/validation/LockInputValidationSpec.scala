package homelab.keyedqueue.domain.service.validation


import homelab.keyedqueue.domain.error.InvalidInput
import homelab.keyedqueue.domain.model.LockClaim
import homelab.keyedqueue.domain.request.v1.{ AcquireRequest, RefreshRequest, ReleaseRequest }
import homelab.keyedqueue.domain.types.*
import zio.*
import zio.test.*


/**
 * That the lock's validation accumulates, and that its ceilings clamp rather than refuse.
 *
 * The clamp is the property an `if` would get wrong two ways: refusing what should be shortened, or
 * granting what should have been bounded. Both ceilings — the wait's and the ttl's — are asserted from
 * the parsed value, not the response.
 */
object LockInputValidationSpec extends ZIOSpecDefault:

  private val validation =
    LockInputValidation(LockInputValidation.Config(maxWait = 30.seconds, maxTtl = 10.minutes))

  /** A claim as a holder would carry it, and the receipt the service would have issued for it. */
  private val claim   = LockClaim(LockName("resource"), Token(7))
  private val receipt = claim.receipt

  def spec: Spec[TestEnvironment & Scope, Any] = suite("LockInputValidation")(
    test("a well-formed acquire passes through unchanged") {
      val parsed = validation.parse(AcquireRequest("resource", ttl = 5.seconds, maxWait = 10.seconds))
      assertTrue(parsed.toEither.exists { acquisition =>
        acquisition.name == LockName("resource")
          && acquisition.ttl == 5.seconds
          && acquisition.patience == 10.seconds
      })
    },
    test("both ceilings clamp: a ttl and a wait beyond them come back as the ceilings") {
      val parsed = validation.parse(AcquireRequest("resource", ttl = 2.hours, maxWait = 5.minutes))
      assertTrue(parsed.toEither.exists { acquisition =>
        acquisition.ttl == 10.minutes && acquisition.patience == 30.seconds
      })
    },
    test("an acquire with three problems is refused once, naming all three") {
      val parsed = validation.parse(AcquireRequest("", ttl = Duration.Zero, maxWait = Duration.Zero))
      assertTrue(
        parsed.toEither.left.exists { problems =>
          problems.toSet == Set[InvalidInput](
            InvalidInput.EmptyLockName,
            InvalidInput.NonPositiveTtl,
            InvalidInput.NonPositiveMaxWait,
          )
        }
      )
    },
    test("a receipt round-trips through release, and a made-up one is refused") {
      val ours   = validation.parse(ReleaseRequest(receipt))
      val theirs = validation.parse(ReleaseRequest("not-a-receipt"))
      assertTrue(
        ours.toEither.exists(_ == claim),
        theirs.toEither.left.exists(_.head == InvalidInput.UnreadableReceipt),
      )
    },
    test("a refresh clamps its ttl like an acquire, against the same claim") {
      val parsed = validation.parse(RefreshRequest(receipt, ttl = 2.hours))
      assertTrue(parsed.toEither.exists((named, ttl) => named == claim && ttl == 10.minutes))
    },
  )
