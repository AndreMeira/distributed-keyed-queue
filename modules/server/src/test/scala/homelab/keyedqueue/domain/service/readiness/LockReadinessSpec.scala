package homelab.keyedqueue.domain.service.readiness


import homelab.keyedqueue.domain.types.LockName
import zio.*
import zio.test.*


/**
 * What a lock's readiness promises a parked waiter.
 *
 * A wake is a broadcast — every waiter on the name is told, and each asks the store for itself — and a
 * mailbox holds one, so a wake landing just before a wait still counts. The tests below drive each of
 * those, and the unsubscribe a closing scope performs.
 */
object LockReadinessSpec extends ZIOSpecDefault:

  private val lock      = LockName("orders")
  private val elsewhere = LockName("elsewhere")

  def spec: Spec[TestEnvironment & Scope, Any] = suite("LockReadiness")(
    test("a wake reaches every waiter on the name") {
      // Where this parts company with the queue's readiness, which hands its token to exactly one caller.
      // Grants go by ticket order and only the store knows whose turn it is, so all of them must look.
      for
        readiness <- LockReadiness.make
        first     <- readiness.subscribe(lock)
        second    <- readiness.subscribe(lock)
        _         <- readiness.ready(lock)
        woken     <- first.await.timeout(1.second)
        also      <- second.await.timeout(1.second)
      yield assertTrue(woken.isDefined, also.isDefined)
    },
    test("a wake reaches no waiter on another name") {
      for
        readiness <- LockReadiness.make
        mine      <- readiness.subscribe(lock)
        other     <- readiness.subscribe(elsewhere)
        _         <- readiness.ready(lock)
        woken     <- mine.await.timeout(1.second)
        quiet     <- other.await.timeout(50.millis)
      yield assertTrue(woken.isDefined, quiet.isEmpty)
    },
    test("a wake that lands before the wait is still found") {
      // What lets a waiter subscribe, ask the store, and only then park without losing the answer.
      for
        readiness <- LockReadiness.make
        signal    <- readiness.subscribe(lock)
        _         <- readiness.ready(lock)
        woken     <- signal.await.timeout(1.second)
      yield assertTrue(woken.isDefined)
    },
    test("two wakes before a wait read as one") {
      // The mailbox slides at one. Sound because a waiter acts on what the store tells it, not on a count.
      for
        readiness <- LockReadiness.make
        signal    <- readiness.subscribe(lock)
        _         <- readiness.ready(lock)
        _         <- readiness.ready(lock)
        woken     <- signal.await.timeout(1.second)
        again     <- signal.await.timeout(50.millis)
      yield assertTrue(woken.isDefined, again.isEmpty)
    },
    test("a wake with nobody subscribed is dropped") {
      // The lock can afford this: a waiter subscribes before it places, so a wake it needs cannot precede it.
      for
        readiness <- LockReadiness.make
        _         <- readiness.ready(lock)
        signal    <- readiness.subscribe(lock)
        woken     <- signal.await.timeout(50.millis)
      yield assertTrue(woken.isEmpty)
    },
    test("readyAll reaches waiters on every name") {
      for
        readiness <- LockReadiness.make
        mine      <- readiness.subscribe(lock)
        other     <- readiness.subscribe(elsewhere)
        _         <- readiness.readyAll
        woken     <- mine.await.timeout(1.second)
        also      <- other.await.timeout(1.second)
      yield assertTrue(woken.isDefined, also.isDefined)
    },
    test("a waiter whose scope has closed is no longer reached") {
      for
        readiness <- LockReadiness.make
        staying   <- readiness.subscribe(lock)
        leaving   <- ZIO.scoped(readiness.subscribe(lock))
        _         <- readiness.ready(lock)
        woken     <- staying.await.timeout(1.second)
        quiet     <- leaving.await.timeout(50.millis)
      yield assertTrue(woken.isDefined, quiet.isEmpty)
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)
