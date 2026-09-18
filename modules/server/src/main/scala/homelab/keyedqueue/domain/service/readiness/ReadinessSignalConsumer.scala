package homelab.keyedqueue.domain.service.readiness


import homelab.common.error.ApplicationError
import homelab.common.messaging.Consumer


/**
 * Where readiness signals arrive from, a batch at a time.
 *
 * An adapter reads whatever its substrate announces and delivers [[ReadinessSignal]]s, so what consumes
 * them works in domain terms only.
 *
 * See `docs/architecture/readiness-and-wake.md`.
 */
trait ReadinessSignalConsumer extends Consumer.Batched[ApplicationError.AdapterError, ReadinessSignal]
