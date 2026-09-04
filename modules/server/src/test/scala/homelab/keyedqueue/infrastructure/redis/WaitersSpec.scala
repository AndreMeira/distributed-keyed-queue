package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.QueueName
import zio.*
import zio.test.*


/**
 * The invariants a signal has to hold.
 *
 * The one that matters most: '''a wake reaches everybody who was listening for it'''. Work is claimable
 * the moment it is announced, so a consumer that misses one is asleep beside work it asked for.
 */
object WaitersSpec extends ZIOSpecDefault:

  private val queue = QueueName("orders")

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Waiters")(
    test("a wake reaches every caller holding the signal") {
      for
        waiters <- Waiters.make
        first   <- waiters.subscribe(queue)
        second  <- waiters.subscribe(queue)
        _       <- waiters.raise(queue)
        one     <- first.await(5.seconds)
        two     <- second.await(5.seconds)
      yield assertTrue(one, two)
    },
    test("a caller that subscribes after a wake waits for the next one") {
      // Not a lost wake: a consumer subscribes before it claims, so whatever this one announced is found
      // by the attempt that follows. Remembering it would wake the next caller for work it already saw.
      for
        waiters <- Waiters.make
        _       <- waiters.raise(queue)
        late    <- waiters.subscribe(queue)
        rang    <- late.await(20.millis)
      yield assertTrue(!rang)
    },
    test("a signal is raised once; the round after it is a new signal") {
      for
        waiters <- Waiters.make
        signal  <- waiters.subscribe(queue)
        _       <- waiters.raise(queue)
        raised  <- signal.await(5.seconds)
        next    <- waiters.subscribe(queue)
        again   <- next.await(20.millis)
      yield assertTrue(raised, !again)
    },
    test("a wake on one queue leaves another queue's callers waiting") {
      for
        waiters <- Waiters.make
        mine    <- waiters.subscribe(queue)
        other   <- waiters.subscribe(QueueName("elsewhere"))
        _       <- waiters.raise(queue)
        raised  <- mine.await(5.seconds)
        quiet   <- other.await(20.millis)
      yield assertTrue(raised, !quiet)
    },
    test("a caller dying around a wake takes nothing from anybody") {
      // The failure the old registry had to work for: there, a wake was handed to one caller, so a caller
      // that died holding one destroyed it. Here it reaches everyone, so a dying caller cannot remove
      // anything — driven from both sides, hard, to show there is no ordering in which it can.
      ZIO
        .foreach(1 to 500): _ =>
          for
            waiters  <- Waiters.make
            doomed   <- waiters.subscribe(queue)
            survivor <- waiters.subscribe(queue)
            waiting  <- doomed.await(5.seconds).fork
            raising  <- waiters.raise(queue).fork
            _        <- waiting.interrupt
            _        <- raising.join
            heard    <- survivor.await(5.seconds)
          yield assertTrue(heard)
        .map(_.reduce(_ && _))
    },
    test("under concurrency every caller holding the signal hears one wake") {
      val callers = 50
      for
        waiters <- Waiters.make
        signals <- ZIO.foreachPar(1 to callers)(_ => waiters.subscribe(queue))
        waiting <- ZIO.foreachPar(signals)(_.await(30.seconds)).fork
        _       <- waiters.raise(queue)
        heard   <- waiting.join
      yield assertTrue(heard.forall(identity), heard.size == callers)
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
