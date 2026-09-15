package homelab.keyedqueue.domain.service.lock


import homelab.keyedqueue.domain.service.lock.DistributedLock.LockName
import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis.RedisSpecSupport
import zio.*
import zio.test.*


/**
 * The lock's guarantees, against a real substrate — proof that the self-client baton mechanism holds.
 *
 * This is the lock built on the queue: one keyed-queue store, a watchdog sweeping it, and the lock's own
 * protocol on top. Every test shares that one instance, so what they exercise is the mechanism itself.
 */
object DistributedLockSpec extends ZIOSpecDefault:

  private val leaseTtl = 2.seconds

  /** A Valkey container for the suite, and the lock over a store and a running watchdog. */
  private val substrate: ZLayer[Any, Any, DistributedLock] =
    RedisSpecSupport.substrate(leaseTtl) >>> ZLayer.scoped:
      for
        config   <- ZIO.service[QueueConfig]
        instance <- RedisSpecSupport.instance(config)
        store     = instance.get[QueueStore]
        watchdog <- Watchdog.make(store, Watchdog.Config(config.sweepInterval, config.sweepLimit))
        // Building a watchdog does not start it, and reclaiming a dead holder needs the loop running.
        _        <- watchdog.run.forkScoped
      yield DistributedLock.make(store, watchdog, leaseTtl)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("DistributedLock")(
    test("a free lock is acquired at once; the same lock held is refused to a second caller") {
      for
        lock  <- ZIO.service[DistributedLock]
        name   = LockName("resource-a")
        first <- lock.tryAcquire(name)
        again <- lock.tryAcquire(name)
      yield assertTrue(first.isDefined, again.isEmpty)
    },
    test("a released lock is takeable again, and the fence rejects a double release") {
      for
        lock     <- ZIO.service[DistributedLock]
        name      = LockName("resource-b")
        held     <- lock.tryAcquire(name).someOrFailException
        released <- lock.release(held)
        // the fence advanced when the claim ended, so releasing the same hold again is refused, not reapplied
        twice    <- lock.release(held)
        retaken  <- lock.tryAcquire(name)
      yield assertTrue(released, !twice, retaken.isDefined)
    },
    test("acquire blocks until the holder releases, then completes") {
      for
        lock      <- ZIO.service[DistributedLock]
        name       = LockName("resource-c")
        held      <- lock.tryAcquire(name).someOrFailException
        waiter    <- lock.acquire(name, 5.seconds).fork
        // give the waiter time to park on the held lock, then release
        _         <- ZIO.sleep(200.millis)
        parked    <- waiter.poll.map(_.isEmpty) // still running == still blocked
        _         <- lock.release(held)
        recovered <- waiter.join
      yield assertTrue(parked, recovered.isDefined)
    },
    test("mutual exclusion holds under contention: never two holders at once") {
      // The property the whole thing exists for. Many fibers hammer one lock; a counter incremented inside
      // the hold and checked against 1 catches any overlap the baton mechanism would have to permit to be wrong.
      val name     = LockName("resource-d")
      val fibers   = 12
      val perFiber = 8
      for
        lock     <- ZIO.service[DistributedLock]
        held     <- Ref.make(0)
        breach   <- Ref.make(false)
        worker    = ZIO
                      .foreachDiscard(1 to perFiber): _ =>
                        lock.withLock(name, 10.seconds):
                          for
                            now <- held.updateAndGet(_ + 1)
                            _   <- breach.set(true).when(now != 1) // anyone else inside == exclusion broken
                            _   <- ZIO.sleep(3.millis)             // widen the window an overlap would use
                            _   <- held.update(_ - 1)
                          yield ()
                      .unit
        _        <- ZIO.foreachParDiscard(1 to fibers)(_ => worker)
        broken   <- breach.get
        leftover <- held.get
      yield assertTrue(!broken, leftover == 0)
    },
    test("a holder that dies is reclaimed, and the lock becomes takeable within the lease") {
      // No explicit release: the hold is dropped on the floor. The store's own lease + sweep is what returns it.
      for
        lock    <- ZIO.service[DistributedLock]
        name     = LockName("resource-e")
        _       <- lock.tryAcquire(name) // taken and abandoned
        blocked <- lock.tryAcquire(name) // still held: none
        // wait past the lease; a retrying caller re-looks and finds it reclaimed
        taken   <- lock.acquire(name, leaseTtl + 3.seconds)
      yield assertTrue(blocked.isEmpty, taken.isDefined)
    },
  ).provideShared(substrate) @@ RedisSpecSupport.againstValkey
