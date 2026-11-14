package homelab.keyedqueue.domain.service.maintenance


import homelab.common.flow.TimeLatch
import zio.*

import java.time.Instant


/**
 * When the next scheduled message may be due: a one-shot latch, swapped for a fresh one each time it fires, and
 * a tick emitted for the mover on every firing.
 *
 * A request is a hint, never the truth: whoever writes due work to the store asks for a tick at its due time,
 * and the mover asks again for the next due time its pass reports. The latch fires at the earliest instant
 * asked for, or after the backstop when nobody asks. Ticks coalesce: the pipe holds at most one, which means
 * "move again", so a tick emitted while the mover is busy is never lost and never doubled.
 *
 * Requests are rounded up to a multiple of the quantum, so entries due within one window share a firing and a
 * pass, at most one quantum late; the latch's slack drops a request that would move it earlier by less than
 * that. See `docs/research/tick-driven-mover.md`.
 *
 * @param latch the current latch, replaced by a fresh one each time it fires
 * @param ticks one pending tick at most, for the mover
 * @param config the quantum, the slack and the backstop
 */
final class DueTimer(latch: Ref[TimeLatch], ticks: Queue[Unit], config: DueTimer.Config):

  /**
   * Fire no later than `delay` from now, rounded up to the quantum.
   *
   * A request landing between a firing and the swap hits the fired latch and changes nothing; it is covered,
   * because it was made after its write to the store and the tick being emitted comes before the next pass.
   *
   * @param delay how long from now, at the latest; a negative one means now
   * @return noop
   */
  def triggerIn(delay: Duration): UIO[Unit] =
    for
      now     <- Clock.instant
      current <- latch.get
      _       <- current.triggerAt(DueTimer.quantised(now.plus(DueTimer.bounded(delay)), config.quantum))
    yield ()

  /**
   * Wait for the next tick.
   *
   * @return noop, once the latch has fired since the last tick was taken
   */
  def tick: UIO[Unit] = ticks.take

  /**
   * Fire, swap, emit, for ever.
   *
   * The order is what makes it correct: a request after the swap lands on the fresh latch, and one before it is
   * covered by the tick that follows.
   *
   * @return never completes
   */
  def run: UIO[Nothing] = cycle.forever

  /**
   * One firing: wait for the current latch, replace it with one armed for the backstop, and emit a tick.
   *
   * @return noop, once the tick is offered
   */
  private def cycle: UIO[Unit] =
    for
      current <- latch.get
      _       <- current.await
      fresh   <- TimeLatch.make(config.backstop, config.slack)
      _       <- latch.set(fresh)
      _       <- ticks.offer(())
    yield ()


object DueTimer:

  /**
   * How the timer fires.
   *
   * @param quantum the window requests are rounded up to; zero or less means no rounding
   * @param slack how much earlier a request must be to move the latch
   * @param backstop the longest the latch waits with no request, which bounds lateness when early requests are
   *                 lost with the instance that held them
   */
  final case class Config(quantum: Duration, slack: Duration, backstop: Duration)

  /**
   * A timer whose first latch fires at once, so the first pass after a boot runs immediately and its answer
   * arms the next latch from the store.
   *
   * Building one allocates its latch and its pipe and nothing else; firing is [[DueTimer.run]], which whoever
   * owns the lifetime forks.
   *
   * @param config the quantum, the slack and the backstop
   * @return the timer
   */
  def make(config: Config): UIO[DueTimer] =
    for
      first <- TimeLatch.make(Duration.Zero, config.slack)
      latch <- Ref.make(first)
      ticks <- Queue.sliding[Unit](1)
    yield DueTimer(latch, ticks, config)

  /**
   * An instant rounded up to the next multiple of the quantum, counted from the epoch.
   *
   * Rounding absolute instants rather than delays is what lets requests made at different moments for the
   * same window share one firing.
   *
   * @param instant the instant asked for
   * @param quantum the window; zero or less leaves the instant as it is
   * @return the rounded instant, never earlier than `instant`
   */
  private def quantised(instant: Instant, quantum: Duration): Instant =
    val window = quantum.toMillis
    if window <= 0 then instant
    else
      val millis = instant.toEpochMilli
      val above  = Math.floorMod(millis, window)
      val rest   = if above == 0 && instant.getNano % 1_000_000 == 0 then 0L else window - above
      Instant.ofEpochMilli(millis + rest)

  /**
   * A delay brought within `[0, 7 days × 1,000]`, far beyond any schedule and far from overflowing an instant
   * or its milliseconds.
   *
   * @param delay the delay asked for
   * @return it, clamped
   */
  private def bounded(delay: Duration): Duration =
    delay.max(Duration.Zero).min(ceiling)

  /** The longest delay [[bounded]] lets through. */
  private val ceiling: Duration = 7000.days
