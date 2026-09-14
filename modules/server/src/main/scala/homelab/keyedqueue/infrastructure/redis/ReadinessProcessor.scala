package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.common.messaging.Consumer
import homelab.common.processing.Processor
import zio.*


/**
 * Where a wake goes: the queue's readiness, the lock's, or both when the reader reports a gap.
 *
 * '''Nothing here knows what a stream is, or which substrate produced one.''' A [[Wake]] already says which
 * kind of thing it names, at the type that kind implies, so this is a match and two sinks — and the error
 * it admits is any adapter's, not Redis's. That is what makes it the half of the wake path that belongs to
 * the domain rather than to the adapter.
 *
 * @param input where wakes come from
 * @param queueReady where queue wakes go
 * @param lockReady where lock wakes go
 */
final class ReadinessProcessor(
  override val input: Consumer[ApplicationError.AdapterError, Wake],
  queueReady: QueueReadiness,
  lockReady: LockReadiness,
) extends Processor[ApplicationError.AdapterError, Wake]:

  /**
   * Deliver one wake to the readiness its kind belongs to.
   *
   * A gap announces every name on both, since a stream carries either kind and the reader cannot say what
   * it stepped over. That over-announcing is the safe direction: a spurious wake costs one look, a missed
   * one costs a waiter asleep beside what it asked for.
   *
   * @param wake what was announced
   * @return noop
   */
  override def process(wake: Wake): IO[ApplicationError.AdapterError, Unit] =
    wake match
      case Wake.Queue(name) => queueReady.ready(name)
      case Wake.Lock(name)  => lockReady.ready(name)
      case Wake.Gap         => queueReady.readyAll *> lockReady.readyAll
