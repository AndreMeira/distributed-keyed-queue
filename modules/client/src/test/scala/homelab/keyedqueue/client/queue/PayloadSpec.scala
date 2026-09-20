package homelab.keyedqueue.client.queue


import zio.*
import zio.schema.codec.{ BinaryCodec, ProtobufCodec }
import zio.schema.{ DeriveSchema, Schema }
import zio.test.*

import java.time.Instant


/**
 * What a caller gets for supplying a schema: a value out and the same value back.
 *
 * The pair is tested together because neither half is worth anything alone — a payload is only readable
 * by something that agrees with whatever wrote it.
 */
object PayloadSpec extends ZIOSpecDefault:

  final case class Order(id: String, lines: Int)

  private given Schema[Order] = DeriveSchema.gen[Order]

  private val order = Order("o-1", 3)

  private def arrived(payload: Chunk[Byte], encoding: String, payloadType: String): Message.Incoming =
    Message.Incoming("k1", MessageId("m1"), payloadType, encoding, payload, Instant.EPOCH, attempt = 1)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("payloads")(
    test("a value written by the schema encoder is read back by the schema decoder") {
      val encoder = MessageEncoder.derive[Order]
      val message = arrived(encoder.encode(order), encoder.encoding, "order.v2")
      assertTrue(MessageDecoder.derive[Order].decode(message) == Right(order))
    },
    test("an outgoing message states the encoding the encoder wrote, never one the caller chose") {
      given MessageEncoder[Order] = MessageEncoder.derive[Order]
      val outgoing                = Message.Outgoing("k1", MessageId("m1"), "order.v2", order)
      assertTrue(
        outgoing.encoding == MessageEncoder.protobuf,
        outgoing.payload.nonEmpty,
        outgoing.payloadType == "order.v2",
      )
    },
    test("bytes that are not the value come back as a failure rather than as something wrong") {
      val message = arrived(Chunk(9.toByte, 9.toByte), MessageEncoder.protobuf, "order.v2")
      assertTrue(MessageDecoder.derive[Order].decode(message).isLeft)
    },
    test("with both autos imported, a caller states the value and nothing about how it travels") {
      import MessageDecoder.auto.given
      import MessageEncoder.auto.given
      val outgoing = Message.Outgoing("k1", MessageId("m1"), "order.v2", order)
      val message  = arrived(outgoing.payload, outgoing.encoding, outgoing.payloadType)
      assertTrue(summon[MessageDecoder[Order]].decode(message) == Right(order))
    },
    test("a decoder over another codec states the format that codec reads") {
      // The schema case cannot disagree with itself, because it does not take the encoding. This one does
      // take it, for a codec this client did not derive, and is the only place the two could part.
      given BinaryCodec[Order] = ProtobufCodec.protobufCodec[Order]
      val decoder              = MessageDecoder.expecting[Order]("order.v2", "application/x-protobuf")
      val message              = arrived(MessageEncoder.derive[Order].encode(order), "application/x-protobuf", "order.v2")
      assertTrue(decoder.decode(message) == Right(order))
    },
    test("a verifying decoder refuses what the sender says is something else") {
      val encoder  = MessageEncoder.derive[Order]
      val decoder  = MessageDecoder.expecting[Order]("order.v2")
      val right    = arrived(encoder.encode(order), MessageEncoder.protobuf, "order.v2")
      val asJson   = arrived(encoder.encode(order), "application/json", "order.v2")
      val asAnItem = arrived(encoder.encode(order), MessageEncoder.protobuf, "item.v1")
      assertTrue(
        decoder.decode(right) == Right(order),
        decoder.decode(asJson) == Left(MessageDecoder.Failure.WrongEncoding(MessageEncoder.protobuf, "application/json")),
        decoder.decode(asAnItem) == Left(MessageDecoder.Failure.WrongType("order.v2", "item.v1")),
      )
    },
  )
