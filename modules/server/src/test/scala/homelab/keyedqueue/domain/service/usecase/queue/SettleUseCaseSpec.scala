package homelab.keyedqueue.domain.service.usecase.queue


import homelab.keyedqueue.domain.model.queue.Claim
import homelab.keyedqueue.domain.model.queue.Settlement.Verdict
import homelab.keyedqueue.domain.request.queue.SettleRequest
import homelab.keyedqueue.domain.request.queue.SettleRequest.MessageOutcome
import homelab.keyedqueue.domain.service.maintenance.{ DueTimer, MaintenanceSpecSupport }
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import homelab.keyedqueue.domain.types.*
import zio.*
import zio.test.*


/**
 * Settling, as far as backoffs go: a settle that asks for a backoff asks the backoff timer for a tick when it ends,
 * so the key is released on time. Over the in-memory store and a real timer on the `TestClock`.
 */
object SettleUseCaseSpec extends ZIOSpecDefault:
  import Support.*

  def spec = suite("SettleUseCase")(
    test("a nack with a backoff asks the timer for a tick when the backoff ends") {
      for
        fixture <- setUp
        _       <- fixture.timer.run.fork
        boot    <- fixture.timer.tick.fork
        _       <- MaintenanceSpecSupport.done(boot, "the boot tick")
        _       <- fixture.settle(nack(5.seconds))
        tick    <- fixture.timer.tick.fork
        _       <- TestClock.adjust(4.seconds)
        early   <- tick.poll
        _       <- TestClock.adjust(1.second)
        _       <- MaintenanceSpecSupport.done(tick, "the tick when the backoff ends")
      yield assertTrue(early.isEmpty)
    },
    test("a settle with no backoff asks for no tick") {
      for
        fixture <- setUp
        _       <- fixture.timer.run.fork
        boot    <- fixture.timer.tick.fork
        _       <- MaintenanceSpecSupport.done(boot, "the boot tick")
        _       <- fixture.settle(nack(Duration.Zero))
        tick    <- fixture.timer.tick.fork
        _       <- TestClock.adjust(1.minute)
        none    <- tick.poll
        _       <- tick.interrupt
        applied <- fixture.store.settled
      yield assertTrue(none.isEmpty, applied.size == 1)
    },
  ) @@ TestAspect.timeout(30.seconds)

  private object Support {

    /**
     * The use case, with the store it settles in and the timer it asks.
     *
     * @param settle the use case
     * @param store what it settles in
     * @param timer what it asks for a tick
     */
    final case class Fixture(settle: SettleUseCase, store: InMemoryQueueStore, timer: DueTimer)

    /**
     * A fresh use case over a fresh store, with a timer that is built but not firing; its backstop is an hour, so
     * only a requested tick comes within a test.
     *
     * @return the fixture
     */
    def setUp: UIO[Fixture] =
      for
        store <- InMemoryQueueStore.make
        timer <- DueTimer.make(DueTimer.Config(Duration.Zero, Duration.Zero, 1.hour))
        settle = SettleUseCase(store, QueueInputValidation(UseCaseSpecSupport.limits), timer)
      yield Fixture(settle, store, timer)

    /**
     * A settle that reports one message failed, with a backoff.
     *
     * @param retryAfter how long the key should wait; zero for none
     * @return the request
     */
    def nack(retryAfter: Duration): SettleRequest =
      SettleRequest(Claim(QueueName("jobs"), MessageKey("k1"), Token(1)).reference, Chunk(MessageOutcome("m1", Verdict.Failed)), retryAfter)
  }
