package homelab.keyedqueue.client.codec


import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration as ProtoDuration
import com.google.protobuf.timestamp.Timestamp
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.*
import homelab.keyedqueue.client.queue.model.{ Claim, Dequeued, Enqueued, Message, MessageId, MessageKey, Receipt, Renewed, Settled, Verdict }
import homelab.keyedqueue.v1
import zio.*
import zio.test.*

import java.time.Instant


/**
 * Reading the wire's answers, where the wire can state more than the client can hold.
 *
 * A claim is a receipt and fields that mean something only when one was granted, so the cases worth
 * testing are the ones where those disagree.
 */
object QueueCodecsSpec extends ZIOSpecDefault:

  private val stamp  = Timestamp(1_700_000_000L, 0)
  private val moment = Instant.ofEpochSecond(1_700_000_000L)
  private val span   = ProtoDuration(30L, 0)

  private def wireMessage(id: String, body: String): v1.Message =
    v1.Message("k1", id, "order.v2", "application/json", Some(stamp), ByteString.copyFromUtf8(body))

  private def delivery(id: String, body: String, attempt: Int = 1): v1.Delivery =
    v1.Delivery(id, Some(wireMessage(id, body)), attempt)

  private def claimed(head: Option[v1.Delivery], tail: Seq[v1.Delivery] = Seq.empty): v1.DequeueResponse =
    v1.DequeueResponse("receipt", head, tail, Some(stamp), 4, Some(span))

  private def claimOf(response: v1.DequeueResponse): Option[Claim] =
    QueueCodecs.decode(response).toOption.flatMap(granted)

  private def granted(dequeued: Dequeued): Option[Claim] =
    dequeued match
      case Dequeued.Claimed(claim) => Some(claim)
      case Dequeued.Idle           => None

  private def unreadable(error: ServiceError): Boolean =
    error match
      case ServiceError.Unreadable(_) => true
      case _                          => false

  def spec: Spec[TestEnvironment & Scope, Any] = suite("QueueCodecs")(
    suite("enqueue")(
      test("a message goes out as an envelope around bytes, stamped with the moment it was sent") {
        val outgoing = Message.Outgoing(MessageKey("k1"), MessageId("m1"), "order.v2", "application/json", Chunk(1.toByte))
        val request  = QueueCodecs.encode("orders", outgoing, moment)
        assertTrue(
          request.queue == "orders",
          request.message.map(_.key).contains("k1"),
          request.message.map(_.messageId).contains("m1"),
          request.message.map(_.payloadType).contains("order.v2"),
          request.message.map(_.encoding).contains("application/json"),
          request.message.flatMap(_.sentAt).map(_.seconds).contains(1_700_000_000L),
          request.message.map(_.payload.toByteArray.toList).contains(List(1.toByte)),
        )
      },
      test("the depth comes back as the service counted it") {
        assertTrue(QueueCodecs.decode(v1.EnqueueResponse(7L)) == Enqueued(7L))
      },
    ),
    suite("dequeue")(
      test("no head is an idle queue, which is an answer rather than an error") {
        assertTrue(QueueCodecs.decode(claimed(head = None)) == Right(Dequeued.Idle))
      },
      test("a claim carries its receipt, its batch in order, and the lease it runs under") {
        val response = claimed(Some(delivery("m1", "first")), Seq(delivery("m2", "second", attempt = 3)))
        val claim    = claimOf(response)
        assertTrue(
          claim.map(_.receipt).contains(Receipt("receipt")),
          claim.map(_.messages.map(_.id).toList).contains(List(MessageId("m1"), MessageId("m2"))),
          claim.map(_.messages.map(_.attempt).toList).contains(List(1, 3)),
          claim.map(_.messages.head.payloadType).contains("order.v2"),
          claim.map(_.messages.head.sentAt).contains(moment),
          claim.map(_.leaseTtl).contains(30.seconds),
          claim.map(_.leaseExpiresAt).contains(moment),
          claim.map(_.backlogDepth).contains(4),
        )
      },
      test("a claim with no receipt is refused: nothing could settle it") {
        val response = claimed(Some(delivery("m1", "first"))).copy(receipt = "")
        assertTrue(QueueCodecs.decode(response).left.exists(unreadable))
      },
      test("a delivery with no message is refused rather than carried inwards") {
        val response = claimed(Some(v1.Delivery("m1", None, 1)))
        assertTrue(QueueCodecs.decode(response).left.exists(unreadable))
      },
      test("a claim with no lease deadline or no span is refused: nothing could time its beats") {
        val noDeadline = claimed(Some(delivery("m1", "first"))).copy(leaseExpiresAt = None)
        val noSpan     = claimed(Some(delivery("m1", "first"))).copy(leaseTtl = None)
        assertTrue(
          QueueCodecs.decode(noDeadline).left.exists(unreadable),
          QueueCodecs.decode(noSpan).left.exists(unreadable),
        )
      },
      test("an unreadable message in the batch is refused the same as one in the head") {
        val response = claimed(Some(delivery("m1", "first")), Seq(v1.Delivery("m2", None, 1)))
        assertTrue(QueueCodecs.decode(response).left.exists(unreadable))
      },
    ),
    suite("settle")(
      test("applied and stale are both answers") {
        assertTrue(
          QueueCodecs.decode(v1.SettleResponse(v1.Applied.APPLIED_OK)) == Right(Settled.Applied),
          QueueCodecs.decode(v1.SettleResponse(v1.Applied.APPLIED_STALE)) == Right(Settled.Stale),
        )
      },
      test("an unspecified outcome says nothing about the claim, so it is refused") {
        assertTrue(QueueCodecs.decode(v1.SettleResponse(v1.Applied.APPLIED_UNSPECIFIED)).left.exists(unreadable))
      },
      test("a verdict names the message and what became of it") {
        assertTrue(
          QueueCodecs.encode(Verdict(MessageId("m1"), Verdict.Outcome.Done)) ==
            v1.MessageOutcome("m1", v1.Outcome.OUTCOME_DONE),
          QueueCodecs.encode(Verdict(MessageId("m2"), Verdict.Outcome.Failed)) ==
            v1.MessageOutcome("m2", v1.Outcome.OUTCOME_FAILED),
        )
      },
    ),
    suite("heartbeat")(
      test("a renewal carries the new deadline, the span it runs for, and what is no longer held") {
        val response = v1.HeartbeatResponse(Seq("gone-1", "gone-2"), Some(stamp), Some(span))
        assertTrue(
          QueueCodecs.decode(response) ==
            Right(Renewed(Chunk(Receipt("gone-1"), Receipt("gone-2")), moment, 30.seconds))
        )
      },
      test("a renewal with no span is refused: the next beat would have nothing to time itself by") {
        val response = v1.HeartbeatResponse(Seq.empty, Some(stamp), None)
        assertTrue(QueueCodecs.decode(response).left.exists(unreadable))
      },
    ),
  )
