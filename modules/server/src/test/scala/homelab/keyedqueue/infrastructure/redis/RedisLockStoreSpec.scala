package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.service.lock.LockStore
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import org.testcontainers.containers.GenericContainer
import zio.*
import zio.test.*


/**
 * The dedicated lock, against real Valkey — and note the layer: '''no watchdog'''. That absence is the
 * point. The self-client lock needed a running sweep to reclaim a dead holder; this reclaims inline on the
 * next acquire, so crash-recovery is proven here with nothing sweeping in the background.
 */
object RedisLockStoreSpec extends ZIOSpecDefault:

  private val substrate: ZLayer[Any, Any, LockStore] =
    ZLayer.scoped:
      for
        container  <- ZIO.acquireRelease(
                        ZIO.attemptBlocking:
                          val _                            = java.lang.System.setProperty("api.version", "1.40")
                          val started: GenericContainer[?] = GenericContainer("valkey/valkey:8.1-alpine")
                          started.setExposedPorts(java.util.List.of(Integer.valueOf(6379)))
                          started.start()
                          started
                      )(container => ZIO.attemptBlocking(container.stop()).ignore)
        url         = s"redis://${container.getHost}:${container.getMappedPort(6379)}"
        config      = QueueConfig(url, cluster = false, 0, 30.seconds, 1.second, 100, 200.millis, 1, 5.seconds, 32)
        connection <- Connection.make(Connection.Config(config.maxWait, config.redisUrl, config.cluster))
        readiness  <- Readiness.make
        store      <- connection.provide(RedisLockStore.make(connection, readiness))
      yield store

  private val ttl = 1.second

  def spec: Spec[TestEnvironment & Scope, Any] = suite("RedisLockStore")(
    test("a free lock is taken; the same lock held is refused") {
      for
        lock  <- ZIO.service[LockStore]
        first <- lock.tryAcquire("a", ttl)
        again <- lock.tryAcquire("a", ttl)
      yield assertTrue(first.isDefined, again.isEmpty)
    },
    test("release frees it, and the fence rejects a double release") {
      for
        lock     <- ZIO.service[LockStore]
        held     <- lock.tryAcquire("b", ttl).someOrFailException
        released <- lock.release(held)
        twice    <- lock.release(held)
        retaken  <- lock.tryAcquire("b", ttl)
      yield assertTrue(released, !twice, retaken.isDefined)
    },
    test("a dead holder is reclaimed inline on the next acquire — no sweep, no watchdog") {
      // The delta from the self-client lock. Take it, abandon it (no release), and prove the *store alone*
      // hands it to the next taker once the lease lapses. Nothing is sweeping here.
      for
        lock    <- ZIO.service[LockStore]
        _       <- lock.tryAcquire("c", ttl)   // taken and dropped on the floor
        blocked <- lock.tryAcquire("c", ttl)   // lease still live: refused
        _       <- ZIO.sleep(ttl + 300.millis) // let the lease lapse
        taken   <- lock.tryAcquire("c", ttl)   // reclaimed inline by this acquire
      yield assertTrue(blocked.isEmpty, taken.isDefined)
    },
    test("refresh extends a held lease and is rejected once the lock is lost") {
      for
        lock      <- ZIO.service[LockStore]
        held      <- lock.tryAcquire("d", ttl).someOrFailException
        (_, ok)   <- lock.refresh(held, ttl) // extends by ttl, under the same token
        // let that refreshed lease lapse; then someone else takes it, advancing the fence
        _         <- ZIO.sleep(ttl + 300.millis)
        stolen    <- lock.tryAcquire("d", ttl).someOrFailException
        (_, lost) <- lock.refresh(held, ttl) // the original holder's refresh now fails
      yield assertTrue(ok, stolen.token > held.token, !lost)
    },
    test("blocking acquire waits for a release, then completes") {
      for
        lock      <- ZIO.service[LockStore]
        held      <- lock.tryAcquire("e", 30.seconds).someOrFailException
        waiter    <- lock.acquire("e", 30.seconds, 5.seconds).fork
        _         <- ZIO.sleep(200.millis)
        parked    <- waiter.poll.map(_.isEmpty)
        _         <- lock.release(held)
        recovered <- waiter.join
      yield assertTrue(parked, recovered.isDefined)
    },
    test("mutual exclusion under contention: never two holders at once") {
      val fibers   = 12
      val perFiber = 6
      for
        lock     <- ZIO.service[LockStore]
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
  ).provideShared(substrate) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.timeout(3.minutes)
