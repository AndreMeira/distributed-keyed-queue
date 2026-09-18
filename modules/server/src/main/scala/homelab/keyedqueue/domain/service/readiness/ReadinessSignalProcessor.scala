package homelab.keyedqueue.domain.service.readiness


import homelab.common.error.ApplicationError
import homelab.common.messaging.Consumer
import homelab.common.processing.Processor
import zio.*


/**
 * Where a wake goes: the queue's readiness, the lock's, or both when the reader reports a gap.
 *
 * A [[ReadinessSignal]] says which kind of thing it names, at the type that kind implies, so delivery is a
 * match over three cases and two sinks. Nothing here names a stream or a substrate.
 *
 * @param input where wakes come from
 * @param queueReadiness where queue wakes go
 * @param lockReadiness where lock wakes go
 */
final class ReadinessSignalProcessor(
  override val input: ReadinessSignalConsumer,
  queueReadiness: QueueReadiness,
  lockReadiness: LockReadiness,
) extends Processor.Batched[ApplicationError.AdapterError, ReadinessSignal]:

  /**
   * Deliver one read's worth of wakes, each to the readiness its kind belongs to.
   *
   * A gap announces every name on both, since a stream carries either kind and the reader cannot say what
   * it stepped over. That over-announcing is the safe direction: a spurious wake costs one look, a missed
   * one costs a waiter asleep beside what it asked for.
   *
   * @param wakes what the read announced, each name once
   * @return noop
   */
  override def process(wakes: List[ReadinessSignal]): IO[ApplicationError.AdapterError, Unit] =
    ZIO.foreachDiscard(wakes):
      case ReadinessSignal.Queue(name) => queueReadiness.ready(name)
      case ReadinessSignal.Lock(name)  => lockReadiness.ready(name)
      case ReadinessSignal.Gap         => queueReadiness.readyAll *> lockReadiness.readyAll
