package homelab.keyedqueue.domain.service.maintenance


import homelab.common.flow.TimeLatch
import zio.*


/**
 * When the next scheduled message may be due: a one-shot latch, swapped for a fresh one each time it fires, and
 * a tick emitted for the mover on every firing.
 *
 * A request is a hint, never the truth: whoever writes due work to the store asks for a tick at its due time,
 * and the mover asks again for the next due time its pass reports. The latch fires at the earliest instant
 * asked for, or after the backstop when nobody asks. Ticks coalesce: the pipe holds at most one, which means
 * "move again", so a tick emitted while the mover is busy is never lost and never doubled.
 *
 * No request is closer than the floor: one asking for less fires one floor from now. Every request on a latch is
 * made after the previous firing, so firings are at least one floor apart however dense the due times are, and
 * entries due within one floor of a pass share the next one. That bounds the ticks, not the store calls: a mover
 * whose pass comes back full repeats it at once, without a tick, so a backlog drains as fast as its passes run. An entry
 * due further out fires on time; only one due within a floor of its request is late, by up to the floor. The
 * latch's slack drops a request that would move it earlier by less than the slack, so a dropped one is up to the
 * slack late. See `docs/research/tick-driven-mover.md`.
 *
 * @param latch the current latch, replaced by a fresh one each time it fires
 * @param ticks one pending tick at most, for the mover
 * @param config the floor, the slack and the backstop
 */
final class DueTimer(latch: Ref[TimeLatch], ticks: Queue[Unit], config: DueTimer.Config):

  /**
   * Fire no later than `delay` from now, and no sooner than the floor.
   *
   * A request landing between a firing and the swap hits the fired latch and changes nothing; it is covered,
   * because it was made after its write to the store and the tick being emitted comes before the next pass.
   *
   * @param delay how long from now, at the latest; one shorter than the floor, negative included, means one floor
   * @return noop
   */
  def triggerIn(delay: Duration): UIO[Unit] =
    for
      now     <- Clock.instant
      current <- latch.get
      _       <- current.triggerAt(now.plus(DueTimer.bounded(delay.max(config.floor))))
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
   * @param floor the shortest delay a request is honoured with, so firings are at least this far apart; zero or
   *              less means none
   * @param slack how much earlier a request must be to move the latch
   * @param backstop the longest the latch waits with no request, which bounds lateness when early requests are
   *                 lost with the instance that held them
   */
  final case class Config(floor: Duration, slack: Duration, backstop: Duration)

  /**
   * A timer whose first latch fires at once, so the first pass after a boot runs immediately and its answer
   * arms the next latch from the store.
   *
   * Building one allocates its latch and its pipe and nothing else; firing is [[DueTimer.run]], which whoever
   * owns the lifetime forks.
   *
   * @param config the floor, the slack and the backstop
   * @return the timer
   */
  def make(config: Config): UIO[DueTimer] =
    for
      first <- TimeLatch.make(Duration.Zero, config.slack)
      latch <- Ref.make(first)
      ticks <- Queue.sliding[Unit](1)
    yield DueTimer(latch, ticks, config)

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
