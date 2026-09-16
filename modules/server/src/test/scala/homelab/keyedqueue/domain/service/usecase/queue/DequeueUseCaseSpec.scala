package homelab.keyedqueue.domain.service.usecase.queue


import homelab.keyedqueue.SpecHelper
import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.domain.request.queue.DequeueRequest
import homelab.keyedqueue.domain.response.queue.DequeueResponse
import homelab.keyedqueue.domain.service.readiness.QueueReadiness
import homelab.keyedqueue.domain.types.QueueName
import zio.*
import zio.test.*


/**
 * Waiting, which is what a dequeue is: attempt, park on the queue's readiness, attempt again until the
 * patience is spent.
 *
 * The store here answers from what a test gave it, so what these assert is the loop rather than any
 * substrate — and a queue that never becomes ready is a caller that waits exactly as long as it asked.
 */
object DequeueUseCaseSpec extends ZIOSpecDefault:

  def spec: Spec[TestEnvironment, Any] = {
    suite("DequeueUseCase")(
      test("work already claimable is handed over without waiting") {
        for
          dequeue         <- ZIO.service[DequeueUseCase]
          store           <- ZIO.service[InMemoryQueueStore]
          waiting          = Helper.grant("ready-now", "k1", "a")
          _               <- store.hasWork(waiting)
          outcome         <- dequeue(DequeueRequest("ready-now", 5.seconds, 1)).timed
          (elapsed, answer) = outcome
        yield assertTrue(answer == DequeueResponse.fromGrant(waiting), elapsed < 1.second)
      },
      test("a queue that never becomes ready answers empty, after the whole patience") {
        for
          dequeue         <- ZIO.service[DequeueUseCase]
          outcome         <- dequeue(DequeueRequest("never", 500.millis, 1)).timed
          (elapsed, answer) = outcome
        yield assertTrue(answer == DequeueResponse.Empty, elapsed >= 500.millis, elapsed < 3.seconds)
      },
      test("work that arrives during the wait is claimed, without waiting out the rest") {
        for
          dequeue         <- ZIO.service[DequeueUseCase]
          store           <- ZIO.service[InMemoryQueueStore]
          readiness       <- ZIO.service[QueueReadiness]
          waiting          = Helper.grant("arrives", "k1", "a")
          asked           <- dequeue(DequeueRequest("arrives", 10.seconds, 1)).timed.fork
          // Long enough that the first look has found nothing and parked.
          _               <- ZIO.sleep(300.millis)
          _               <- store.hasWork(waiting)
          _               <- readiness.ready(QueueName("arrives"))
          outcome         <- asked.join
          (elapsed, answer) = outcome
        yield assertTrue(answer == DequeueResponse.fromGrant(waiting), elapsed < 5.seconds)
      },
      test("the patience is a deadline: fruitless looks do not extend it") {
        // Announced repeatedly with nothing to find. Each wake costs a look, and a look that finds nothing
        // must not buy the caller more time than it asked for.
        for
          dequeue         <- ZIO.service[DequeueUseCase]
          readiness       <- ZIO.service[QueueReadiness]
          announcing      <- readiness.ready(QueueName("busy")).delay(100.millis).forever.fork
          outcome         <- dequeue(DequeueRequest("busy", 600.millis, 1)).timed
          _               <- announcing.interrupt
          (elapsed, answer) = outcome
        yield assertTrue(answer == DequeueResponse.Empty, elapsed >= 600.millis, elapsed < 3.seconds)
      },
    ) @@ SpecHelper.Aspect.common
  }.provide(UseCaseSpecSupport.layer)
