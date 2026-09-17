package homelab.keyedqueue.domain.service.usecase.lock


import homelab.keyedqueue.domain.service.persistence.LockStore
import homelab.keyedqueue.domain.service.readiness.LockReadiness
import homelab.keyedqueue.domain.service.validation.LockInputValidation
import zio.*


/**
 * The acquire use case over whatever store and readiness a spec provides.
 *
 * The waiting is the use case's, so a spec that wants to exercise it builds this over a real store: what
 * the ticket ordering and the wake path do together is only visible with both.
 */
object UseCaseSpecSupport {

  /** The ceilings a parsed demand is held to, wide enough that these specs are never clamped. */
  val limits: LockInputValidation.Config = LockInputValidation.Config(maxWait = 60.seconds, maxTtl = 10.minutes)

  /**
   * The use case, over the store and readiness already in the environment.
   *
   * @return the layer
   */
  lazy val layer: ZLayer[LockStore & LockReadiness, Nothing, LockAcquireUseCase] =
    ZLayer.fromFunction: (store: LockStore, readiness: LockReadiness) =>
      LockAcquireUseCase(store, LockInputValidation(limits), readiness)
}
