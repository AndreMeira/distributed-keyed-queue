package homelab.keyedqueue.e2e


import zio.*
import zio.test.*


/**
 * The lock as it is deployed: two instances, one Valkey, clients over the wire.
 *
 * What only this suite can show: that a hold is a fact about the '''store''', not about the instance that
 * granted it — released through the other instance, surviving the granting instance's death — and that a
 * waiter parked on one instance is granted by events on, or reported through, the other. The contract
 * under test is `docs/architecture/lock-guarantees.md`; the invariant names below are its.
 */
object LockSpec extends ZIOSpecDefault:

  def spec: Spec[TestEnvironment & Scope, Any] = suite("the lock, deployed")(
    test("held through one instance is held on both; released through either; the fence rises (M1, M3)") {
      for
        dkq     <- ZIO.service[Deployment]
        name     = dkq.lock("exclusive")
        first   <- dkq.a.acquire(name, ttl = 30.seconds, patience = 1.second)
        refused <- dkq.b.acquire(name, ttl = 30.seconds, patience = 1.second)
        // The receipt is a fact about the store: instance b can release a hold granted through a.
        freed   <- dkq.b.release(first.receipt)
        second  <- dkq.b.acquire(name, ttl = 30.seconds, patience = 5.seconds)
        _       <- dkq.b.release(second.receipt)
      yield assertTrue(
        first.acquired,
        !refused.acquired,
        freed,
        second.acquired,
        second.fence > first.fence,
      )
    },
    test("a release on one instance grants the waiter parked on the other (A2)") {
      for
        dkq               <- ZIO.service[Deployment]
        name               = dkq.lock("handover")
        held              <- dkq.a.acquire(name, ttl = 30.seconds, patience = 1.second)
        waiter            <- dkq.b.acquire(name, ttl = 30.seconds, patience = 15.seconds).timed.fork
        _                 <- ZIO.sleep(500.millis)
        _                 <- dkq.a.release(held.receipt)
        (took, granted)   <- waiter.join
        _                 <- dkq.b.release(granted.receipt)
      yield assertTrue(
        granted.acquired,
        took > 400.millis, // it really waited on a held lock…
        took < 10.seconds, // …and was woken by the release, not by its patience
      )
    },
    test("a dead holder delays but cannot orphan the lock, and its stale calls are refused (A5, H3, H4)") {
      // The holder vanishes without releasing: no wake will ever announce this lock. The waiter is granted
      // anyway, at lease expiry — and the dead holder's receipt is then an answer, not a power.
      for
        dkq             <- ZIO.service[Deployment]
        name             = dkq.lock("estate")
        held            <- dkq.a.acquire(name, ttl = 3.seconds, patience = 1.second)
        waiter          <- dkq.b.acquire(name, ttl = 30.seconds, patience = 20.seconds).timed.fork
        (took, granted) <- waiter.join
        staleRelease    <- dkq.a.release(held.receipt)
        staleRefresh    <- dkq.a.refresh(held.receipt, ttl = 5.seconds)
        _               <- dkq.b.release(granted.receipt)
      yield assertTrue(
        granted.acquired,
        granted.fence > held.fence,
        took < 10.seconds, // granted around the 3s lease expiry, well before the 20s patience
        !staleRelease,
        !staleRefresh.renewed,
      )
    },
    test("the hold and its queue survive the instance that granted them (M1 across a death)") {
      // Kill the instance that granted the hold and hosts nothing else the test needs: the hold, the
      // waiter's place, and the receipt all live in the store, so the surviving instance serves them all.
      for
        dkq             <- ZIO.service[Deployment]
        name             = dkq.lock("bereaved")
        held            <- dkq.a.acquire(name, ttl = 30.seconds, patience = 1.second)
        waiter          <- dkq.b.acquire(name, ttl = 30.seconds, patience = 20.seconds).timed.fork
        _               <- ZIO.sleep(500.millis)
        _               <- Compose.kill(dkq.a.name)
        freed           <- dkq.b.release(held.receipt)
        (took, granted) <- waiter.join
        _               <- dkq.b.release(granted.receipt)
      yield assertTrue(
        freed,
        granted.acquired,
        granted.fence > held.fence,
        took < 10.seconds,
      )
    } @@ TestAspect.after(Compose.revive("dkq-a").orDie) @@ TestAspect.ifEnvNotSet("DKQ_E2E_ENDPOINTS"),
  ).provideShared(Deployment.layer)
    @@ TestAspect.withLiveClock // every wait here is a real one; a virtual clock would prove nothing
    @@ TestAspect.sequential   // the tests share a deployment, and one of them kills half of it
    @@ TestAspect.timeout(10.minutes)
