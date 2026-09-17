package homelab.keyedqueue.domain.model.queue


import homelab.keyedqueue.domain.types.*
import zio.Scope
import zio.test.*


/**
 * What a receipt has to survive: any content a caller can put in a queue name or a key.
 *
 * Keys come from the caller's domain, so the encoding cannot reserve a character. Each field is encoded
 * separately and joined with a dot, which base64url cannot produce — so the cases below are about content
 * that used to collide with the separator.
 */
object ClaimSpec extends ZIOSpecDefault:

  private def roundTrips(claim: Claim) =
    assertTrue(Claim.decode(claim.reference).contains(claim))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Claim")(
    test("a reference reads back") {
      roundTrips(Claim(QueueName("orders"), MessageKey("k-1"), Token(7)))
    },
    test("a queue name holding a space reads back") {
      roundTrips(Claim(QueueName("my orders"), MessageKey("k-1"), Token(7)))
    },
    test("a key holding a space reads back") {
      roundTrips(Claim(QueueName("orders"), MessageKey("order 42 line 1"), Token(7)))
    },
    test("a key holding the separator reads back") {
      roundTrips(Claim(QueueName("a.b"), MessageKey("c.d"), Token(7)))
    },
    test("every field empty but named still reads back") {
      roundTrips(Claim(QueueName("o"), MessageKey(""), Token(0)))
    },
    test("a reference this service did not issue is refused") {
      assertTrue(
        Claim.decode("not-a-receipt").isEmpty,
        Claim.decode("").isEmpty,
        Claim.decode("a.b").isEmpty,
        Claim.decode("a.b.c.d").isEmpty,
      )
    },
    test("a reference whose token is not a number is refused") {
      val forged = Seq("orders", "k-1", "not-a-token")
        .map(p => java.util.Base64.getUrlEncoder.withoutPadding.encodeToString(p.getBytes("UTF-8")))
        .mkString(".")
      assertTrue(Claim.decode(forged).isEmpty)
    },
  )
