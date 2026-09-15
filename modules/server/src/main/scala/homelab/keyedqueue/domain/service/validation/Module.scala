package homelab.keyedqueue.domain.service.validation

import zio.ZLayer


/** Wiring for the validation concern. */
object Module:

  /** The two parses, one per API. */
  type Provided = QueueInputValidation & LockInputValidation

  /** What each parse is bounded by. */
  type Required = QueueInputValidation.Config & LockInputValidation.Config

  /**
   * Both parses, as one layer.
   *
   * @return the layer
   */
  lazy val layer: ZLayer[Required, Nothing, Provided] = input ++ lockInput

  /**
   * The parse, and the limits it holds callers to.
   *
   * A layer rather than a constant so that the day a check needs the store, only this file changes: its
   * requirement grows and every use case keeps asking for the same thing. Today that requirement is the
   * service's bounds, which the parse enforces by building a `Demand` that cannot exceed them.
   *
   * @return the layer
   */
  val input: ZLayer[QueueInputValidation.Config, Nothing, QueueInputValidation] =
    ZLayer.fromFunction(QueueInputValidation.apply)

  /**
   * The lock's parse, and the bound it holds callers to.
   *
   * @return the layer
   */
  val lockInput: ZLayer[LockInputValidation.Config, Nothing, LockInputValidation] =
    ZLayer.fromFunction(LockInputValidation.apply)
