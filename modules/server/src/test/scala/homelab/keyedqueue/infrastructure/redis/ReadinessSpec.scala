package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.QueueName
import zio.*
import zio.test.*


/**
 * The invariants a readiness token has to hold, all of them about races.
 *
 * The one that matters most: '''a token is never lost while there is work to find'''. Work is claimable
 * the moment it is announced, so a token that vanishes is a queue going quiet with messages sitting in it.
 * Every path that could swallow one puts one back, and the tests below drive each of those paths.
 */
object ReadinessSpec extends ZIOSpecDefault:

  private val queue = QueueName("orders")

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Readiness")(
    test("a token wakes one caller, not every caller") {
      // The whole point of the design: a broadcast would let both look.
      for
        readiness <- Readiness.make
        _         <- readiness.await(queue, 1.second)(ZIO.none) // spend the seed
        looked    <- Ref.make(0)
        _         <- readiness.ready(queue)
        _         <- ZIO.foreachPar(1 to 2)(_ => readiness.await(queue, 150.millis)(looked.update(_ + 1).as(None)))
        count     <- looked.get
      yield assertTrue(count == 1)
    },
    test("a queue nobody has announced is still looked at once") {
      // The restart case: the wake stream is positioned at its end, so work already in `ready` would never
      // be announced. A fresh queue carries one token so its first caller looks instead of waiting.
      for
        readiness <- Readiness.make
        looked    <- Ref.make(0)
        found     <- readiness.await(QueueName("cold"), 50.millis)(looked.update(_ + 1).as(Some(1)))
        count     <- looked.get
      yield assertTrue(found.contains(1), count == 1)
    },
    test("finding work hands the token on; finding nothing stops the chain") {
      // What replaces the broadcast: a burst drains one consumer at a time. The chain must end on the first
      // look that finds nothing, or consumers spin on the store for as long as they are waiting.
      for
        readiness <- Readiness.make
        looked    <- Ref.make(0)
        first     <- readiness.await(queue, 50.millis)(looked.update(_ + 1).as(Some(1)))
        second    <- readiness.await(queue, 50.millis)(looked.update(_ + 1).as(Some(2)))
        third     <- readiness.await(queue, 50.millis)(looked.update(_ + 1).as(None))
        fourth    <- readiness.await(queue, 50.millis)(looked.update(_ + 1).as(Some(4)))
        count     <- looked.get
      yield assertTrue(first.contains(1), second.contains(2), third.isEmpty, fourth.isEmpty, count == 3)
    },
    test("a queue announced for is not confused with another") {
      for
        readiness <- Readiness.make
        _         <- readiness.await(queue, 1.second)(ZIO.none)
        _         <- readiness.await(QueueName("elsewhere"), 1.second)(ZIO.none)
        _         <- readiness.ready(queue)
        mine      <- readiness.await(queue, 100.millis)(ZIO.succeed(Some(1)))
        other     <- readiness.await(QueueName("elsewhere"), 50.millis)(ZIO.succeed(Some(1)))
      yield assertTrue(mine.contains(1), other.isEmpty)
    },
    test("readiness announced as the patience expires is not lost") {
      // `take.timeout` forks the take, so an element arriving as the timeout fires can be swallowed by a
      // child fiber nothing observes. Driven from both sides: whichever wins, the work stays findable.
      ZIO
        .foreach(1 to 500): _ =>
          for
            readiness <- Readiness.make
            _         <- readiness.await(queue, 1.second)(ZIO.none)
            // Claims rather than looking-and-finding-nothing: a fruitless look would consume the token
            // legitimately, which is not what this test is about.
            awaiting  <- readiness.await(queue, 20.millis)(ZIO.succeed(Some(1))).fork
            _         <- ZIO.sleep(20.millis)
            _         <- readiness.ready(queue)
            first     <- awaiting.join
            second    <- readiness.await(queue, 3.seconds)(ZIO.succeed(Some(1)))
          yield assertTrue(first.isDefined || second.isDefined)
        .map(_.reduce(_ && _))
    },
    test("readiness announced as the caller is interrupted is not lost") {
      // The path the patience cannot reach: the whole `await` is cancelled, so neither its exit handler nor
      // its recovery runs. Only the finaliser outside the timeout covers this one.
      ZIO
        .foreach(1 to 500): _ =>
          for
            readiness  <- Readiness.make
            _          <- readiness.await(queue, 1.second)(ZIO.none)
            awaiting   <- readiness.await(queue, 30.seconds)(ZIO.succeed(Some(1))).fork
            _          <- ZIO.sleep(1.milli)
            announcing <- readiness.ready(queue).fork
            exit       <- awaiting.interrupt
            _          <- announcing.join
            later      <- readiness.await(queue, 3.seconds)(ZIO.succeed(Some(1)))
          yield assertTrue(exit.isSuccess || later.isDefined)
        .map(_.reduce(_ && _))
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(3.minutes)
