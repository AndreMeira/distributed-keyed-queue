package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.SpecHelper
import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.domain.model.Acquisition
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.types.LockName
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import zio.*
import zio.test.*


/**
 * The dedicated lock, against real Valkey.
 *
 * Two things to notice in the harness. There is '''no watchdog''' — crash-recovery is inline reclaim on the
 * next grant, so nothing sweeps. And each instance runs its own wake path over the shared lock stream into
 * its own [[LockReadiness]], so a release on one instance wakes every waiter on another — the
 * cross-instance and fairness tests below turn on exactly that, with nothing polling.
 */
object RedisLockStoreSpec extends ZIOSpecDefault:

  private val ttl = 1.second

  def spec: Spec[TestEnvironment & Scope, Any] = {
    suite("RedisLockStore")(
      test("a free lock is taken; the same lock held is refused") {
        for
          lock  <- ZIO.service[LockStore]
          first <- lock.tryAcquire(Helper.acq("a", ttl))
          again <- lock.tryAcquire(Helper.acq("a", ttl))
        yield assertTrue(first.isDefined, again.isEmpty)
      },
      test("release frees it, and the fence rejects a double release") {
        for
          lock     <- ZIO.service[LockStore]
          held     <- lock.tryAcquire(Helper.acq("b", ttl)).someOrFailException
          released <- lock.release(held.claim)
          twice    <- lock.release(held.claim)
          retaken  <- lock.tryAcquire(Helper.acq("b", ttl))
        yield assertTrue(released, !twice, retaken.isDefined)
      },
      test("a dead holder is reclaimed inline on the next acquire — no sweep, no watchdog") {
        for
          lock    <- ZIO.service[LockStore]
          _       <- lock.tryAcquire(Helper.acq("c", ttl))
          blocked <- lock.tryAcquire(Helper.acq("c", ttl))
          _       <- ZIO.sleep(ttl + 300.millis)
          taken   <- lock.tryAcquire(Helper.acq("c", ttl))
        yield assertTrue(blocked.isEmpty, taken.isDefined)
      },
      test("refresh extends a held lease and is rejected once the lock is lost") {
        for
          lock      <- ZIO.service[LockStore]
          held      <- lock.tryAcquire(Helper.acq("d", ttl)).someOrFailException
          (_, ok)   <- lock.refresh(held.claim, ttl)
          _         <- ZIO.sleep(ttl + 300.millis)
          stolen    <- lock.tryAcquire(Helper.acq("d", ttl)).someOrFailException
          (_, lost) <- lock.refresh(held.claim, ttl)
        yield assertTrue(ok, stolen.claim.token > held.claim.token, !lost)
      },
      test("blocking acquire waits for a release, then completes") {
        for
          lock      <- ZIO.service[LockStore]
          held      <- lock.tryAcquire(Helper.acq("e", 30.seconds)).someOrFailException
          waiter    <- lock.acquire(Helper.acq("e", 30.seconds, 5.seconds)).fork
          _         <- ZIO.sleep(300.millis)
          parked    <- waiter.poll.map(_.isEmpty)
          _         <- lock.release(held.claim)
          recovered <- waiter.join
        yield assertTrue(parked, recovered.isDefined)
      },
      test("a release on one instance wakes a blocked waiter on another — cross-instance, no polling") {
        // The property the wake stream exists for. Hold on A, block on B; B has no in-process signal, so if
        // it wakes it is because A's release reached the shared stream and B's listener delivered it. A tight
        // patience with headroom over the wake round trip: it must acquire well before it, not by timing out.
        for
          config            <- ZIO.service[QueueConfig]
          a                 <- ZIO.service[LockStore]
          b                 <- ZIO.service[LockStore]
          held              <- a.tryAcquire(Helper.acq("cross", 30.seconds)).someOrFailException
          waiter            <- b.acquire(Helper.acq("cross", 30.seconds, 10.seconds)).timed.fork
          _                 <- ZIO.sleep(300.millis)
          parked            <- waiter.poll.map(_.isEmpty)
          _                 <- a.release(held.claim)
          (took, recovered) <- waiter.join
        yield assertTrue(parked, recovered.isDefined, took < 5.seconds)
      },
      test("trim honours the grace: a late holder may still refresh, an abandoned one is removed") {
        for
          lock      <- ZIO.service[LockStore]
          held      <- lock.tryAcquire(Helper.acq("g", 300.millis)).someOrFailException
          _         <- ZIO.sleep(600.millis)
          // Expired, but within a generous grace: not trimmed, and the late holder may still extend.
          early     <- lock.trim(grace = 1.hour, limit = 10)
          (_, ok)   <- lock.refresh(held.claim, 300.millis)
          _         <- ZIO.sleep(600.millis)
          // Expired beyond a tiny grace: abandoned, so the hold is removed and the fence entry with it.
          freed     <- lock.trim(grace = 100.millis, limit = 10)
          (_, lost) <- lock.refresh(held.claim, 1.second)
          retaken   <- lock.tryAcquire(Helper.acq("g", ttl))
        yield assertTrue(early.isEmpty, ok, freed.contains(LockName("g")), !lost, retaken.isDefined)
      },
      test("a waiter parked on a dead holder reclaims at lease expiry — no release, no trim, no polling") {
        // The deadline-aware wait: the enter is refused with "re-check when the lease ends", so the waiter
        // wakes itself at exactly the moment the lock becomes reclaimable, with no wake ever arriving.
        for
          config            <- ZIO.service[QueueConfig]
          a                 <- ZIO.service[LockStore]
          b                 <- ZIO.service[LockStore]
          _                 <- a.tryAcquire(Helper.acq("h", 500.millis)).someOrFailException
          waiter            <- b.acquire(Helper.acq("h", 30.seconds, 10.seconds)).timed.fork
          _                 <- ZIO.sleep(200.millis)
          parked            <- waiter.poll.map(_.isEmpty)
          (took, recovered) <- waiter.join
        yield assertTrue(parked, recovered.isDefined, took < 2.seconds)
      },
      test("grants follow arrival order — first to enter is first granted") {
        for
          lock   <- ZIO.service[LockStore]
          held   <- lock.tryAcquire(Helper.acq("fifo", 30.seconds)).someOrFailException
          order  <- Ref.make(Chunk.empty[Int])
          turn    = (i: Int) =>
                      lock.acquire(Helper.acq("fifo", 5.seconds, 20.seconds)).flatMap {
                        case Some(hold) => order.update(_ :+ i) *> lock.release(hold.claim)
                        case None       => ZIO.unit
                      }
          first  <- turn(1).fork
          _      <- ZIO.sleep(200.millis)
          second <- turn(2).fork
          _      <- ZIO.sleep(200.millis)
          third  <- turn(3).fork
          _      <- ZIO.sleep(200.millis)
          _      <- lock.release(held.claim)
          _      <- first.join *> second.join *> third.join
          served <- order.get
        yield assertTrue(served == Chunk(1, 2, 3))
      },
      test("a newcomer cannot barge past a waiter: tryAcquire never wins through a handover") {
        for
          lock    <- ZIO.service[LockStore]
          held    <- lock.tryAcquire(Helper.acq("barge", 30.seconds)).someOrFailException
          waiter  <- lock.acquire(Helper.acq("barge", 30.seconds, 10.seconds)).fork
          _       <- ZIO.sleep(300.millis)
          barged  <- Ref.make(false)
          newcomer = lock.tryAcquire(Helper.acq("barge", 30.seconds)).flatMap(r => barged.set(true).when(r.isDefined))
          spam    <- newcomer.repeat(Schedule.spaced(5.millis)).fork
          _       <- lock.release(held.claim)
          granted <- waiter.join
          _       <- ZIO.sleep(300.millis)
          _       <- spam.interrupt
          sneaked <- barged.get
        yield assertTrue(granted.isDefined, !sneaked)
      },
      test("mutual exclusion under contention: never two holders at once") {
        val fibers   = 12
        val perFiber = 2
        for
          lock     <- ZIO.service[LockStore]
          inside   <- Ref.make(0)
          breach   <- Ref.make(false)
          worker    = ZIO.foreachDiscard(1 to perFiber): _ =>
                        ZIO.acquireReleaseWith(lock.acquire(Helper.acq("f", 30.seconds, 30.seconds)))(h =>
                          ZIO.foreachDiscard(h)(held => lock.release(held.claim).ignore)
                        ) {
                          case Some(_) =>
                            for
                              n <- inside.updateAndGet(_ + 1)
                              _ <- breach.set(true).when(n != 1)
                              _ <- ZIO.sleep(3.millis)
                              _ <- inside.update(_ - 1)
                            yield ()
                          case None    => ZIO.unit
                        }
          _        <- ZIO.foreachParDiscard(1 to fibers)(_ => worker)
          broken   <- breach.get
          leftover <- inside.get
        yield assertTrue(!broken, leftover == 0)
      },
    ) @@ SpecHelper.Aspect.common @@ RedisSpecSupport.Aspect.init
  }.provideSomeShared[Scope](RedisSpecSupport.config(30.seconds) >+> RedisSpecSupport.layer)
