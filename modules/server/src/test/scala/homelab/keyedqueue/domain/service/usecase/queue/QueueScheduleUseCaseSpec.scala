package homelab.keyedqueue.domain.service.usecase.queue


import homelab.common.error.ValidationError
import homelab.keyedqueue.SpecHelper.Helper
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
