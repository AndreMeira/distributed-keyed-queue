package homelab.keyedqueue.domain.service.validation

import zio.ZLayer


/** Wiring for the validation concern. */
object Module:

  /**
   * The input validator, which needs nothing to do its job today.
   *
   * A layer rather than a constant so that the day a check needs the store, only this file changes: its
   * requirement grows and every use case keeps asking for the same thing.
   *
   * @return the layer
   */
  val input: ZLayer[Any, Nothing, QueueInputValidation] = ZLayer.succeed(QueueInputValidation())
