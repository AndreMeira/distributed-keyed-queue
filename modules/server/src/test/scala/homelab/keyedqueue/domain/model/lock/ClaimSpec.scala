package homelab.keyedqueue.domain.model.lock


import homelab.keyedqueue.domain.types.*
import zio.Scope
import zio.test.*


/**
 * What a lock receipt has to survive: any content a caller can put in a lock name.
 *
 * The name is encoded on its own and joined to the token with a dot, which base64url cannot produce — so
 * the cases below are about content that used to collide with the separator.
 */
object ClaimSpec extends ZIOSpecDefault:

  private def roundTrips(claim: Claim) =
    assertTrue(Claim.decode(claim.reference).contains(claim))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Claim")(
    test("a reference reads back") {
      roundTrips(Claim(LockName("resource"), Token(7)))
    },
    test("a lock name holding a space reads back") {
      roundTrips(Claim(LockName("my resource"), Token(7)))
    },
    test("a lock name holding the separator reads back") {
      roundTrips(Claim(LockName("a.b.c"), Token(7)))
    },
    test("a reference this service did not issue is refused") {
      assertTrue(
        Claim.decode("not-a-receipt").isEmpty,
        Claim.decode("").isEmpty,
        Claim.decode("a").isEmpty,
        Claim.decode("a.b.c").isEmpty,
      )
    },
  )
