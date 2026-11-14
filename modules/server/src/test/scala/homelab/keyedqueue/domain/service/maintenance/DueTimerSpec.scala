package homelab.keyedqueue.domain.service.maintenance


import zio.*
import zio.test.*


/**
 * The schedule timer against the `TestClock`: when it ticks, what a request changes, and that ticks coalesce.
 *
 * Every test takes the boot tick first: once it is taken, the timer has fired its first latch and swapped in the
 * next one, so a request lands on a latch that can still move.
 */
object DueTimerSpec extends ZIOSpecDefault:
  import MaintenanceSpecSupport.done
  import Support.*

  def spec = suite("DueTimer")(
    test("the first tick comes at once, so a pass runs as soon as an instance boots") {
      for
        timer <- DueTimer.make(DueTimer.Config(Duration.Zero, Duration.Zero, 1.hour))
        _     <- timer.run.fork
        boot  <- timer.tick.fork
        _     <- done(boot, "the boot tick")
      yield assertCompletes
    },
    test("a request brings the next tick to its time, and not before") {
      for
        timer <- booted(DueTimer.Config(Duration.Zero, Duration.Zero, 1.hour))
        _     <- timer.triggerIn(10.seconds)
        tick  <- timer.tick.fork
        _     <- TestClock.adjust(9.seconds)
        early <- tick.poll
        _     <- TestClock.adjust(1.second)
        _     <- done(tick, "the tick")
      yield assertTrue(early.isEmpty)
    },
    test("a request is rounded up to the quantum, so entries due within one window share a tick") {
      for
        timer <- booted(DueTimer.Config(50.millis, Duration.Zero, 1.hour))
        _     <- timer.triggerIn(10.millis)
        tick  <- timer.tick.fork
        _     <- TestClock.adjust(40.millis)
        early <- tick.poll
        _     <- TestClock.adjust(10.millis)
        _     <- done(tick, "the tick")
      yield assertTrue(early.isEmpty)
    },
    test("with no request, the backstop ticks") {
      for
        timer <- booted(DueTimer.Config(Duration.Zero, Duration.Zero, 30.seconds))
        tick  <- timer.tick.fork
        _     <- TestClock.adjust(29.seconds)
        early <- tick.poll
        _     <- TestClock.adjust(1.second)
        _     <- done(tick, "the tick")
      yield assertTrue(early.isEmpty)
    },
    test("two firings nobody has taken a tick for are one tick: ticks coalesce, and none is lost") {
      for
        timer  <- booted(DueTimer.Config(Duration.Zero, Duration.Zero, 1.hour))
        _      <- timer.triggerIn(1.second)
        _      <- TestClock.adjust(1.second)
        _      <- timer.triggerIn(1.second)
        _      <- TestClock.adjust(1.second)
        first  <- timer.tick.fork
        _      <- done(first, "the coalesced tick")
        second <- timer.tick.fork
        _      <- ZIO.yieldNow.repeatN(100)
        extra  <- second.poll
        _      <- second.interrupt
      yield assertTrue(extra.isEmpty)
    },
  ) @@ TestAspect.timeout(30.seconds)

  private object Support {

    /**
     * A timer firing, with its boot tick already taken.
     *
     * @param config how it fires
     * @return the timer; fails when the boot tick does not come
     */
    def booted(config: DueTimer.Config): IO[String, DueTimer] =
      for
        timer <- DueTimer.make(config)
        _     <- timer.run.fork
        boot  <- timer.tick.fork
        _     <- done(boot, "the boot tick")
      yield timer
  }
