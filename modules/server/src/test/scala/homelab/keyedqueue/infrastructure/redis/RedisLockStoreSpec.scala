package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.model.Acquisition
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.domain.types.LockName
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import org.testcontainers.containers.GenericContainer
import zio.*
import zio.test.*


/**
 * The dedicated lock, against real Valkey.
 *
 * Two things to notice in the harness. There is '''no watchdog''' — crash-recovery is inline reclaim on the
 * next grant, so nothing sweeps. And each instance runs its own [[WakeListener]] over the shared lock wake
 * stream into a [[Broadcast]], so a release on one instance wakes every waiter on another — the
 * cross-instance and fairness tests below turn on exactly that, with nothing polling.
 */
object RedisLockStoreSpec extends ZIOSpecDefault:

  private val ttl = 1.second

  /** An acquisition for a name — patience defaults to none, which `tryAcquire` ignores anyway. */
  private def acq(name: String, ttl: Duration, patience: Duration = Duration.Zero): Acquisition =
    Acquisition(LockName(name), ttl, patience)

  /** A Valkey container for the suite, and the config to build lock instances over it. */
  private val substrate: ZLayer[Any, Any, QueueConfig] =
    ZLayer.scoped:
      for
        container <- ZIO.acquireRelease(
                       ZIO.attemptBlocking:
                         val _                            = java.lang.System.setProperty("api.version", "1.40")
                         val started: GenericContainer[?] = GenericContainer("valkey/valkey:8.1-alpine")
                         started.setExposedPorts(java.util.List.of(Integer.valueOf(6379)))
                         started.start()
                         started
                     )(container => ZIO.attemptBlocking(container.stop()).ignore)
        url        = s"redis://${container.getHost}:${container.getMappedPort(6379)}"
      yield QueueConfig(url, cluster = false, 0, 30.seconds, 1.second, 100, 120.seconds, 10.minutes, 10.minutes, 200.millis, 5.seconds, 32)

  /**
   * One lock instance: its own connection, readiness, store, and a listener over the shared lock wake
   * stream — a stand-in for one pod. Two of these over one container are two independent instances.
   *
   * @param config where Redis is
   * @return the store; its listener forked for the caller's scope
   */
  private def instance(config: QueueConfig): ZIO[Scope, ApplicationError, LockStore] =
    for
      connection <- Connection.make(
                      Connection.Config(config.maxWait, config.redisUrl, config.cluster),
                      Chunk(LockKeys.wake),
                    )
      broadcast  <- Broadcast.make
      store      <- connection.provide(RedisLockStore.make(Monitor.Noop, connection, broadcast))
      listener   <- WakeListener.make(connection, config.wakeBlock, Map(LockKeys.wake -> broadcast))
      _          <- listener.run.forkScoped
    yield store

  def spec: Spec[TestEnvironment & Scope, Any] = suite("RedisLockStore")(
    test("a free lock is taken; the same lock held is refused") {
      for
        lock  <- ZIO.serviceWithZIO[QueueConfig](instance)
        first <- lock.tryAcquire(acq("a", ttl))
        again <- lock.tryAcquire(acq("a", ttl))
      yield assertTrue(first.isDefined, again.isEmpty)
    },
    test("release frees it, and the fence rejects a double release") {
      for
        lock     <- ZIO.serviceWithZIO[QueueConfig](instance)
        held     <- lock.tryAcquire(acq("b", ttl)).someOrFailException
        released <- lock.release(held.claim)
        twice    <- lock.release(held.claim)
        retaken  <- lock.tryAcquire(acq("b", ttl))
      yield assertTrue(released, !twice, retaken.isDefined)
    },
    test("a dead holder is reclaimed inline on the next acquire — no sweep, no watchdog") {
      for
        lock    <- ZIO.serviceWithZIO[QueueConfig](instance)
        _       <- lock.tryAcquire(acq("c", ttl))
        blocked <- lock.tryAcquire(acq("c", ttl))
        _       <- ZIO.sleep(ttl + 300.millis)
        taken   <- lock.tryAcquire(acq("c", ttl))
      yield assertTrue(blocked.isEmpty, taken.isDefined)
    },
    test("refresh extends a held lease and is rejected once the lock is lost") {
      for
        lock      <- ZIO.serviceWithZIO[QueueConfig](instance)
        held      <- lock.tryAcquire(acq("d", ttl)).someOrFailException
        (_, ok)   <- lock.refresh(held.claim, ttl)
        _         <- ZIO.sleep(ttl + 300.millis)
        stolen    <- lock.tryAcquire(acq("d", ttl)).someOrFailException
        (_, lost) <- lock.refresh(held.claim, ttl)
      yield assertTrue(ok, stolen.claim.token > held.claim.token, !lost)
    },
    test("blocking acquire waits for a release, then completes") {
      for
        lock      <- ZIO.serviceWithZIO[QueueConfig](instance)
        held      <- lock.tryAcquire(acq("e", 30.seconds)).someOrFailException
        waiter    <- lock.acquire(acq("e", 30.seconds, 5.seconds)).fork
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
        a                 <- instance(config)
        b                 <- instance(config)
        held              <- a.tryAcquire(acq("cross", 30.seconds)).someOrFailException
        waiter            <- b.acquire(acq("cross", 30.seconds, 10.seconds)).timed.fork
        _                 <- ZIO.sleep(300.millis)
        parked            <- waiter.poll.map(_.isEmpty)
        _                 <- a.release(held.claim)
        (took, recovered) <- waiter.join
      yield assertTrue(parked, recovered.isDefined, took < 5.seconds)
    },
    test("trim honours the grace: a late holder may still refresh, an abandoned one is removed") {
      for
        lock      <- ZIO.serviceWithZIO[QueueConfig](instance)
        held      <- lock.tryAcquire(acq("g", 300.millis)).someOrFailException
        _         <- ZIO.sleep(600.millis)
        // Expired, but within a generous grace: not trimmed, and the late holder may still extend.
        early     <- lock.trim(grace = 1.hour, limit = 10)
        (_, ok)   <- lock.refresh(held.claim, 300.millis)
        _         <- ZIO.sleep(600.millis)
        // Expired beyond a tiny grace: abandoned, so the hold is removed and the fence entry with it.
        freed     <- lock.trim(grace = 100.millis, limit = 10)
        (_, lost) <- lock.refresh(held.claim, 1.second)
        retaken   <- lock.tryAcquire(acq("g", ttl))
      yield assertTrue(early.isEmpty, ok, freed.contains(LockName("g")), !lost, retaken.isDefined)
    },
    test("a waiter parked on a dead holder reclaims at lease expiry — no release, no trim, no polling") {
      // The deadline-aware wait: the enter is refused with "re-check when the lease ends", so the waiter
      // wakes itself at exactly the moment the lock becomes reclaimable, with no wake ever arriving.
      for
        config            <- ZIO.service[QueueConfig]
        a                 <- instance(config)
        b                 <- instance(config)
        _                 <- a.tryAcquire(acq("h", 500.millis)).someOrFailException
        waiter            <- b.acquire(acq("h", 30.seconds, 10.seconds)).timed.fork
        _                 <- ZIO.sleep(200.millis)
        parked            <- waiter.poll.map(_.isEmpty)
        (took, recovered) <- waiter.join
      yield assertTrue(parked, recovered.isDefined, took < 2.seconds)
    },
    test("grants follow arrival order — first to enter is first granted") {
      for
        lock   <- ZIO.serviceWithZIO[QueueConfig](instance)
        held   <- lock.tryAcquire(acq("fifo", 30.seconds)).someOrFailException
        order  <- Ref.make(Chunk.empty[Int])
        turn    = (i: Int) =>
                    lock.acquire(acq("fifo", 5.seconds, 20.seconds)).flatMap {
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
        lock    <- ZIO.serviceWithZIO[QueueConfig](instance)
        held    <- lock.tryAcquire(acq("barge", 30.seconds)).someOrFailException
        waiter  <- lock.acquire(acq("barge", 30.seconds, 10.seconds)).fork
        _       <- ZIO.sleep(300.millis)
        barged  <- Ref.make(false)
        newcomer = lock.tryAcquire(acq("barge", 30.seconds)).flatMap(r => barged.set(true).when(r.isDefined))
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
      val perFiber = 6
      for
        lock     <- ZIO.serviceWithZIO[QueueConfig](instance)
        inside   <- Ref.make(0)
        breach   <- Ref.make(false)
        worker    = ZIO.foreachDiscard(1 to perFiber): _ =>
                      ZIO.acquireReleaseWith(lock.acquire(acq("f", 30.seconds, 30.seconds)))(h =>
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
  ).provideSomeShared[Scope](substrate) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.timeout(3.minutes)
