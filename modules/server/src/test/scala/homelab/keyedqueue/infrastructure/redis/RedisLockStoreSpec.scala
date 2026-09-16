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
    ) @@ RedisSpecSupport.Aspect.init @@ SpecHelper.Aspect.common
  }.provideSomeShared[Scope](RedisSpecSupport.config(30.seconds) >+> RedisSpecSupport.layer)
