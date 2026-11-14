package homelab.keyedqueue.domain.service.usecase.queue


import homelab.common.error.ValidationError
import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.domain.model.queue.ScheduledSubmission
import homelab.keyedqueue.domain.request.queue.ScheduleRequest
import homelab.keyedqueue.domain.response.queue.ScheduleResponse
import homelab.keyedqueue.domain.service.maintenance.{ DueTimer, MaintenanceSpecSupport, Watchdog }
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import zio.*
import zio.test.*


/**
 * Scheduling a message: what the use case records, what it refuses, and that it asks the timer for a tick at
 * the due time. Over the in-memory store and a real timer on the `TestClock`.
 */
object QueueScheduleUseCaseSpec extends ZIOSpecDefault:
  import Support.*

  def spec = suite("QueueScheduleUseCase")(
    test("a schedule is recorded, and answered with its due time") {
      for
        fixture  <- setUp
        response <- fixture.schedule(ScheduleRequest("jobs", Helper.requestMessage("k1"), 10.seconds))
        recorded <- fixture.store.scheduled
      yield assertTrue(
        response == ScheduleResponse(scheduled = true, dueIn = 10.seconds),
        recorded.map(_.delay) == Chunk(10.seconds),
      )
    },
    test("a schedule with something wrong is refused, every problem named, and nothing is recorded") {
      for
        fixture  <- setUp
        refused  <- fixture.schedule(ScheduleRequest("", Helper.requestMessage("k1"), Duration.Zero)).flip
        recorded <- fixture.store.scheduled
      yield assertTrue(
        refused match
          case invalid: ValidationError => invalid.errors.size == 2
          case _                        => false
        ,
        recorded.isEmpty,
      )
    },
    test("the schedule asks the timer for a tick at its due time") {
      for
        fixture <- setUp
        _       <- fixture.timer.run.fork
        boot    <- fixture.timer.tick.fork
        _       <- MaintenanceSpecSupport.done(boot, "the boot tick")
        _       <- fixture.schedule(ScheduleRequest("jobs", Helper.requestMessage("k1"), 10.seconds))
        tick    <- fixture.timer.tick.fork
        _       <- TestClock.adjust(9.seconds)
        early   <- tick.poll
        _       <- TestClock.adjust(1.second)
        _       <- MaintenanceSpecSupport.done(tick, "the tick at the due time")
      yield assertTrue(early.isEmpty)
    },
    test("the tick is asked for the delay less the store's reply time, so a slow reply does not make it late") {
      // The store records, then takes 500 ms to answer: the due time was set when it recorded, so the tick must
      // come 10 s after the call, not 10 s after the reply.
      for
        fixture <- setUp
        _       <- fixture.timer.run.fork
        boot    <- fixture.timer.tick.fork
        _       <- MaintenanceSpecSupport.done(boot, "the boot tick")
        release <- Promise.make[Nothing, Unit]
        _       <- fixture.store.holdWritesUntil(release)
        call    <- fixture.schedule(ScheduleRequest("jobs", Helper.requestMessage("k1"), 10.seconds)).fork
        _       <- recordedWithin(fixture.store, MaintenanceSpecSupport.patience)
        _       <- TestClock.adjust(500.millis)
        _       <- release.succeed(())
        _       <- MaintenanceSpecSupport.done(call, "the schedule")
        tick    <- fixture.timer.tick.fork
        _       <- TestClock.adjust(9500.millis)
        _       <- MaintenanceSpecSupport.done(tick, "the tick at 10 s from the call")
      yield assertCompletes
    },
    test("a schedule cancelled after the store recorded it still asks for its tick") {
      // The store holds the call open just after writing: a cancellation then lands between the write and the
      // tick request. Split there, the schedule would wait for the 1 h backstop while this instance is healthy.
      for
        fixture <- setUp
        _       <- fixture.timer.run.fork
        boot    <- fixture.timer.tick.fork
        _       <- MaintenanceSpecSupport.done(boot, "the boot tick")
        release <- Promise.make[Nothing, Unit]
        _       <- fixture.store.holdWritesUntil(release)
        call    <- fixture.schedule(ScheduleRequest("jobs", Helper.requestMessage("k1"), 10.seconds)).fork
        _       <- recordedWithin(fixture.store, MaintenanceSpecSupport.patience)
        cancel  <- call.interrupt.fork
        _       <- release.succeed(())
        _       <- MaintenanceSpecSupport.done(cancel, "the cancellation")
        tick    <- fixture.timer.tick.fork
        _       <- TestClock.adjust(10.seconds)
        _       <- MaintenanceSpecSupport.done(tick, "the tick for the recorded schedule")
      yield assertCompletes
    },
  ) @@ TestAspect.timeout(30.seconds)

  private object Support {

    /**
     * The use case, with the store it records in and the timer it asks.
     *
     * @param schedule the use case
     * @param store what it records in
     * @param timer what it asks for a tick
     */
    final case class Fixture(schedule: QueueScheduleUseCase, store: InMemoryQueueStore, timer: DueTimer)

    /**
     * Wait until the store has recorded a schedule, or fail.
     *
     * @param store the store
     * @param left how many more yields the wait may take
     * @return noop once a schedule is recorded; fails when none is within the patience
     */
    def recordedWithin(store: InMemoryQueueStore, left: Int): IO[String, Unit] =
      store.scheduled.flatMap(recordedOr(store, left))

    /**
     * What a look at the store found, and what follows.
     *
     * @param store the store
     * @param left how many more yields the wait may take
     * @param recorded the schedules recorded so far
     * @return noop when one is; another look after a yield; or a failure when the patience is gone
     */
    def recordedOr(store: InMemoryQueueStore, left: Int)(recorded: Chunk[ScheduledSubmission]): IO[String, Unit] =
      if recorded.nonEmpty then ZIO.unit
      else if left > 0 then ZIO.yieldNow *> recordedWithin(store, left - 1)
      else ZIO.fail("the schedule was not recorded")

    /**
     * A fresh use case over a fresh store, with a timer that is built but not firing.
     *
     * @return the fixture
     */
    def setUp: UIO[Fixture] =
      for
        store    <- InMemoryQueueStore.make
        watchdog <- Watchdog.make(store, UseCaseSpecSupport.sweeping)
        timer    <- DueTimer.make(DueTimer.Config(Duration.Zero, Duration.Zero, 1.hour))
        schedule  = QueueScheduleUseCase(store, watchdog, QueueInputValidation(UseCaseSpecSupport.limits), timer)
      yield Fixture(schedule, store, timer)
  }
