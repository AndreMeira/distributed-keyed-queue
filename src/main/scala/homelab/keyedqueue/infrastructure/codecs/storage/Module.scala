package homelab.keyedqueue.infrastructure.codecs.storage


import homelab.keyedqueue.domain.service.serialisation.EnvelopeCodec
import zio.ZLayer


/** Wiring for how a message is serialised into the store. */
object Module:

  /**
   * The protobuf codec, which stores a message exactly as it arrived.
   *
   * @return the layer
   */
  val envelopeCodec: ZLayer[Any, Nothing, EnvelopeCodec] = ZLayer.succeed(ProtobufEnvelopeCodec())
