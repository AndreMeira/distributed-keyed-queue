package homelab.keyedqueue.domain.service.readiness


import homelab.common.error.ApplicationError
import homelab.common.messaging.Consumer
import homelab.common.processing.Processor
import zio.*


/**
 * Where a wake goes: the queue's readiness, the lock's, or both when the reader reports a gap.
 *
 * A [[Wake]] says which kind of thing it names, at the type that kind implies, so delivery is a match over
 * three cases and two sinks. Nothing here names a stream or a substrate.
 *
 * @param input where wakes come from
 * @param queueReady where queue wakes go
 * @param lockReady where lock wakes go
 */
final class ReadinessProcessor(
  override val input: Consumer.Batched[ApplicationError.AdapterError, Wake],
  queueReady: QueueReadiness,
  lockReady: LockReadiness,
) extends Processor.Batched[ApplicationError.AdapterError, Wake]:

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
  override def process(wakes: List[Wake]): IO[ApplicationError.AdapterError, Unit] =
    ZIO.foreachDiscard(wakes)(delivered)

  /**
   * Deliver one wake to the readiness its kind belongs to.
   *
   * @param wake what was announced
   * @return noop
   */
  private def delivered(wake: Wake): UIO[Unit] =
    wake match
      case Wake.Queue(name) => queueReady.ready(name)
      case Wake.Lock(name)  => lockReady.ready(name)
      case Wake.Gap         => queueReady.readyAll *> lockReady.readyAll
