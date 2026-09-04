package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.QueueName
import zio.*
import zio.test.*


/**
 * The invariants a wake-up registry has to hold, all of them about races.
 *
 * The one that matters most: '''a wake is never lost while somebody wants it'''. Work is claimable the
 * moment it is announced, so a dropped wake is a consumer asleep beside work it asked for.
 */
object WaitersSpec extends ZIOSpecDefault:

  private val queue = QueueName("orders")

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Waiters")(
    test("a wake goes to the longest-waiting caller, and only to one") {
      for
        waiters <- Waiters.make
        first   <- waiters.waitFor(queue, 5.seconds).fork
        _       <- waitUntil(waiters, 1)
        second  <- waiters.waitFor(queue, 5.seconds).fork
        _       <- waitUntil(waiters, 2)
        _       <- waiters.wake(queue)
        woken   <- first.join
        pending <- second.poll
      yield assertTrue(woken, pending.isEmpty)
    },
    test("a wake with nobody waiting is remembered, and taken without sleeping") {
      for
        waiters <- Waiters.make
        _       <- waiters.wake(queue)
        woken   <- waiters.waitFor(queue, 10.millis) // far shorter than the wake would take to arrive
      yield assertTrue(woken)
    },
    test("the remembered wake is taken once, not repeatedly") {
      for
        waiters <- Waiters.make
        _       <- waiters.wake(queue)
        first   <- waiters.waitFor(queue, 10.millis)
        second  <- waiters.waitFor(queue, 10.millis)
      yield assertTrue(first, !second)
    },
    test("a caller that gave up does not absorb a wake meant for a live one") {
      for
        waiters <- Waiters.make
        gaveUp  <- waiters.waitFor(queue, 20.millis) // registers, times out, stays in the queue
        live    <- waiters.waitFor(queue, 5.seconds).fork
        _       <- waitUntil(waiters, 1)
        _       <- waiters.wake(queue)
        woken   <- live.join
      yield assertTrue(!gaveUp, woken)
    },
    test("a wake is never lost when a caller dies around it") {
      // The race the promise CAS exists for, driven from both sides: the wake and the interrupt are fired
      // together, so across enough rounds each ordering happens. Whichever wins, the invariant is the same
      // — either the dying caller used the wake, or it is still there for the next one. Never neither.
      ZIO
        .foreach(1 to 200): _ =>
          for
            waiters <- Waiters.make
            caller  <- waiters.waitFor(queue, 5.seconds).fork
            _       <- waitUntil(waiters, 1)
            ringing <- waiters.wake(queue).fork
            exit    <- caller.interrupt
            used     = exit match
                         case Exit.Success(woken) => woken
                         case _                   => false
            // Joined before asking, not because the ordering matters — it is the point of the test — but
            // because the invariant is about the state once both have happened. Without this the wake can
            // still be queued when the question is asked, and a slow scheduler answers "neither" for a
            // wake that simply had not been rung yet.
            _       <- ringing.join
            later   <- waiters.waitFor(queue, 50.millis)
          yield assertTrue(used || later)
        .map(_.reduce(_ && _))
    },
    test("under concurrency every wake reaches a caller") {
      // Fifty waiters, fifty wakes, from many fibers at once: none may be lost or double-delivered.
      val callers = 50
      for
        waiters <- Waiters.make
        parked  <- ZIO.foreachPar(1 to callers)(_ => waiters.waitFor(queue, 30.seconds)).fork
        _       <- waitUntil(waiters, callers)
        _       <- ZIO.foreachParDiscard(1 to callers)(_ => waiters.wake(queue))
        woken   <- parked.join
        left    <- waiters.waiting(queue)
      yield assertTrue(woken.forall(identity), woken.size == callers, left == 0)
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)

  /** Wait until `count` callers are registered, so a test can wake them deterministically. */
  private def waitUntil(waiters: Waiters, count: Int): UIO[Unit] =
    waiters.waiting(queue).repeatUntil(_ >= count).unit
