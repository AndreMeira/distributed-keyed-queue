package homelab.keyedqueue.domain.service.readiness


import homelab.keyedqueue.domain.service.readiness.QueueReadiness.Signal
import homelab.keyedqueue.domain.types.QueueName
import zio.*
import zio.test.*


/**
 * The invariants a readiness token has to hold, all of them about races.
 *
 * The one that matters most: a token is never lost while there is work to find. Work is claimable the
 * moment it is announced, so a token that vanishes is a queue going quiet with messages sitting in it. Every
 * path a wait can take puts one back, and the tests below drive each of those paths. What a taker does with
 * a token is the use case's, and is tested there.
 */
object QueueReadinessSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("QueueReadiness")(
    test("a token wakes one caller, not every caller") {
      // The whole point of the design: a broadcast would let both take one.
      for
        readiness <- QueueReadiness.make
        signal    <- readiness.subscribe(queue)
        _         <- signal.await(1.second) // spend the seed
        _         <- readiness.ready(queue)
        taken     <- ZIO.foreachPar(1 to 2)(_ => signal.await(150.millis))
      yield assertTrue(taken.count(identity) == 1)
    },
    test("a queue nobody has announced is still looked at once") {
      // The restart case: the wake stream is positioned at its end, so work already in `ready` would never
      // be announced. A fresh queue carries one token so its first caller looks instead of waiting.
      for
        readiness <- QueueReadiness.make
        signal    <- readiness.subscribe(QueueName("cold"))
        taken     <- signal.await(50.millis)
      yield assertTrue(taken)
    },
    test("a wait that gives up leaves a token for the next") {
      // The timeout forks the take and cannot say whether the fork won, so a give-up puts one back.
      for
        readiness <- QueueReadiness.make
        signal    <- readiness.subscribe(queue)
        _         <- signal.await(1.second) // spend the seed
        gaveUp    <- signal.await(50.millis)
        next      <- signal.await(50.millis)
      yield assertTrue(!gaveUp, next)
    },
    test("a queue announced for is not confused with another") {
      for
        readiness <- QueueReadiness.make
        mine      <- readiness.subscribe(queue)
        other     <- readiness.subscribe(QueueName("elsewhere"))
        _         <- mine.await(1.second)
        _         <- other.await(1.second)
        _         <- readiness.ready(queue)
        taken     <- mine.await(100.millis)
        quiet     <- other.await(50.millis)
      yield assertTrue(taken, !quiet)
    },
    test("readiness announced as the patience expires is not lost") {
      // `take.timeout` forks the take, so an element arriving as the timeout fires can be swallowed by a
      // child fiber nothing observes. Driven from both sides: whichever wins, the token stays takeable —
      // by the racing waiter, or failing that by the next one. A taken token is consumed, so the next one
      // only asks when the racer came away empty.
      ZIO
        .foreach(1 to 500): _ =>
          for
            readiness <- QueueReadiness.make
            signal    <- readiness.subscribe(queue)
            _         <- signal.await(1.second)
            awaiting  <- signal.await(20.millis).fork
            _         <- ZIO.sleep(20.millis)
            _         <- readiness.ready(queue)
            taken     <- awaiting.join.flatMap(first => if first then ZIO.succeed(true) else signal.await(3.seconds))
          yield assertTrue(taken)
        .map(_.reduce(_ && _))
    },
    test("a token announced as a caller is interrupted around it is not stranded from a retrying caller") {
      // Interrupt a consumer in the window where a wake arrives — the path neither the timeout nor a
      // handler reaches — then check what the store relies on: a caller that keeps asking gets a token.
      ZIO
        .foreach(1 to 500): _ =>
          for
            readiness  <- QueueReadiness.make
            signal     <- readiness.subscribe(queue)
            _          <- signal.await(1.second)
            awaiting   <- signal.await(30.seconds).fork
            _          <- ZIO.sleep(1.milli)
            announcing <- readiness.ready(queue).fork
            _          <- awaiting.interrupt
            _          <- announcing.join
            recovered  <- retrying(signal).timeout(10.seconds)
          yield assertTrue(recovered.contains(true))
        .map(_.reduce(_ && _))
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(3.minutes)

  /** The queue these tests wait on, and a caller that keeps asking. */
  private object Support {

    val queue = QueueName("orders")

    /** Ask until a token is taken, the way a dequeue loops until granted or its patience is spent. */
    def retrying(signal: Signal): UIO[Boolean] =
      signal.await(100.millis).flatMap(taken => if taken then ZIO.succeed(true) else retrying(signal))
  }
