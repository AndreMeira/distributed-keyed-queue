package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.infrastructure.redis.script.LockKeys
import org.testcontainers.containers.GenericContainer
import zio.*
import zio.test.*


/**
 * The dedicated lock, against real Valkey.
 *
 * Two things to notice in the harness. There is '''no watchdog''' — crash-recovery is inline reclaim on the
 * next acquire, so nothing sweeps. And each instance runs its own [[WakeListener]] over the shared lock
 * wake stream, so a release on one instance wakes a waiter on another — the cross-instance test below turns
 * on exactly that, with nothing polling.
 */
object RedisLockStoreSpec extends ZIOSpecDefault:

  private val ttl = 1.second

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
      yield QueueConfig(url, cluster = false, 0, 30.seconds, 1.second, 100, 200.millis, 1, 5.seconds, 32)

  /**
   * One lock instance: its own connection, readiness, store, and a listener over the shared lock wake
   * stream — a stand-in for one pod. Two of these over one container are two independent instances.
   *
   * @param config where Redis is
   * @return the store; its listener forked for the caller's scope
   */
  private def instance(config: QueueConfig): ZIO[Scope, ApplicationError, LockStore] =
    for
      connection <- Connection.make(Connection.Config(config.maxWait, config.redisUrl, config.cluster))
      readiness  <- Readiness.make
      store      <- connection.provide(RedisLockStore.make(connection, readiness))
      listener   <- WakeListener.make(connection, config.wakeBlock, Map(LockKeys.wake -> readiness))
      _          <- listener.run.forkScoped
    yield store

  def spec: Spec[TestEnvironment & Scope, Any] = suite("RedisLockStore")(
    test("a free lock is taken; the same lock held is refused") {
      for
        lock  <- ZIO.serviceWithZIO[QueueConfig](instance)
        first <- lock.tryAcquire("a", ttl)
        again <- lock.tryAcquire("a", ttl)
      yield assertTrue(first.isDefined, again.isEmpty)
    },
    test("release frees it, and the fence rejects a double release") {
      for
        lock     <- ZIO.serviceWithZIO[QueueConfig](instance)
        held     <- lock.tryAcquire("b", ttl).someOrFailException
        released <- lock.release(held)
        twice    <- lock.release(held)
        retaken  <- lock.tryAcquire("b", ttl)
      yield assertTrue(released, !twice, retaken.isDefined)
    },
    test("a dead holder is reclaimed inline on the next acquire — no sweep, no watchdog") {
      for
        lock    <- ZIO.serviceWithZIO[QueueConfig](instance)
        _       <- lock.tryAcquire("c", ttl)
        blocked <- lock.tryAcquire("c", ttl)
        _       <- ZIO.sleep(ttl + 300.millis)
        taken   <- lock.tryAcquire("c", ttl)
      yield assertTrue(blocked.isEmpty, taken.isDefined)
    },
    test("refresh extends a held lease and is rejected once the lock is lost") {
      for
        lock      <- ZIO.serviceWithZIO[QueueConfig](instance)
        held      <- lock.tryAcquire("d", ttl).someOrFailException
        (_, ok)   <- lock.refresh(held, ttl)
        _         <- ZIO.sleep(ttl + 300.millis)
        stolen    <- lock.tryAcquire("d", ttl).someOrFailException
        (_, lost) <- lock.refresh(held, ttl)
      yield assertTrue(ok, stolen.token > held.token, !lost)
    },
    test("blocking acquire waits for a release, then completes") {
      for
        lock      <- ZIO.serviceWithZIO[QueueConfig](instance)
        held      <- lock.tryAcquire("e", 30.seconds).someOrFailException
        waiter    <- lock.acquire("e", 30.seconds, 5.seconds).fork
        _         <- ZIO.sleep(300.millis)
        parked    <- waiter.poll.map(_.isEmpty)
        _         <- lock.release(held)
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
        held              <- a.tryAcquire("cross", 30.seconds).someOrFailException
        waiter            <- b.acquire("cross", 30.seconds, 10.seconds).timed.fork
        _                 <- ZIO.sleep(300.millis)
        parked            <- waiter.poll.map(_.isEmpty)
        _                 <- a.release(held)
        (took, recovered) <- waiter.join
      yield assertTrue(parked, recovered.isDefined, took < 5.seconds)
    },
    test("mutual exclusion under contention: never two holders at once") {
      val fibers   = 12
      val perFiber = 6
      for
        lock     <- ZIO.serviceWithZIO[QueueConfig](instance)
        inside   <- Ref.make(0)
        breach   <- Ref.make(false)
        worker    = ZIO.foreachDiscard(1 to perFiber): _ =>
                      ZIO.acquireReleaseWith(lock.acquire("f", 30.seconds, 30.seconds))(h => ZIO.foreachDiscard(h)(lock.release(_).ignore)) {
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
