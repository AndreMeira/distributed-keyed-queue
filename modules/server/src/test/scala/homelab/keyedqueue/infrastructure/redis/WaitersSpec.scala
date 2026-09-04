package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.QueueName
import zio.*
import zio.test.*


/**
 * The invariants a bell has to hold.
 *
 * The one that matters most: '''a ring reaches everybody who was listening for it'''. Work is claimable
 * the moment it is announced, so a consumer that misses a ring is asleep beside work it asked for.
 */
object WaitersSpec extends ZIOSpecDefault:

  private val queue = QueueName("orders")

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Waiters")(
    test("a ring reaches every caller holding the bell") {
      for
        waiters <- Waiters.make
        first   <- waiters.subscribe(queue)
        second  <- waiters.subscribe(queue)
        _       <- waiters.wake(queue)
        one     <- first.await(5.seconds)
        two     <- second.await(5.seconds)
      yield assertTrue(one, two)
    },
    test("a caller that subscribes after a ring waits for the next one") {
      // Not a lost ring: a consumer subscribes before it claims, so whatever this ring announced is found
      // by the attempt that follows. Remembering it would wake the next caller for work it already saw.
      for
        waiters <- Waiters.make
        _       <- waiters.wake(queue)
        late    <- waiters.subscribe(queue)
        rang    <- late.await(20.millis)
      yield assertTrue(!rang)
    },
    test("a bell rings once; the round after it is a new bell") {
      for
        waiters <- Waiters.make
        bell    <- waiters.subscribe(queue)
        _       <- waiters.wake(queue)
        rang    <- bell.await(5.seconds)
        next    <- waiters.subscribe(queue)
        again   <- next.await(20.millis)
      yield assertTrue(rang, !again)
    },
    test("a ring on one queue leaves another queue's callers waiting") {
      for
        waiters <- Waiters.make
        mine    <- waiters.subscribe(queue)
        other   <- waiters.subscribe(QueueName("elsewhere"))
        _       <- waiters.wake(queue)
        rang    <- mine.await(5.seconds)
        quiet   <- other.await(20.millis)
      yield assertTrue(rang, !quiet)
    },
    test("a caller dying around a ring takes nothing from anybody") {
      // The failure the old registry had to work for: there, a wake was handed to one caller, so a caller
      // that died holding one destroyed it. Here a ring is a broadcast, so a dying caller cannot remove
      // anything — driven from both sides, hard, to show there is no ordering in which it can.
      ZIO
        .foreach(1 to 500): _ =>
          for
            waiters  <- Waiters.make
            doomed   <- waiters.subscribe(queue)
            survivor <- waiters.subscribe(queue)
            waiting  <- doomed.await(5.seconds).fork
            ringing  <- waiters.wake(queue).fork
            _        <- waiting.interrupt
            _        <- ringing.join
            heard    <- survivor.await(5.seconds)
          yield assertTrue(heard)
        .map(_.reduce(_ && _))
    },
    test("under concurrency every caller holding the bell hears one ring") {
      val callers = 50
      for
        waiters <- Waiters.make
        bells   <- ZIO.foreachPar(1 to callers)(_ => waiters.subscribe(queue))
        waiting <- ZIO.foreachPar(bells)(_.await(30.seconds)).fork
        _       <- waiters.wake(queue)
        heard   <- waiting.join
      yield assertTrue(heard.forall(identity), heard.size == callers)
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
