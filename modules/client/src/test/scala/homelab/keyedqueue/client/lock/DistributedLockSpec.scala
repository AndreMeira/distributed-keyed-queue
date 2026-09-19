package homelab.keyedqueue.client.lock


import homelab.keyedqueue.client.LockError
import zio.*
import zio.test.*

import java.time.Instant


/**
 * What the managed form promises beyond the client: the lease is kept alive while the caller's effect
 * runs, and the lock comes back on every exit.
 */
object DistributedLockSpec extends ZIOSpecDefault:

  /** The lease the fake service grants, which is what the renewals are timed by. */
  private val lease = 40.millis

  private val hold = Hold(Receipt("handle"), Fence(1L), Instant.EPOCH, lease)

  /** A client that answers as told and records what it was asked to do. */
  final private class Fake(
    granted: Option[Hold],
    refreshes: Ref[Chunk[Duration]],
    releases: Ref[Int],
    renewed: Promise[Nothing, Unit],
    lostAfter: Int,
    granting: Option[Duration],
  ) extends LockClient:
    override def acquire(name: String, ttl: Duration, maxWait: Duration): IO[LockError, Acquired] =
      ZIO.succeed(granted.fold(Acquired.Unavailable)(Acquired.Granted.apply))
    override def tryAcquire(name: String, ttl: Duration): IO[LockError, Acquired]                 =
      acquire(name, ttl, Duration.Zero)
    override def release(receipt: Receipt): IO[LockError, Boolean]                                =
      releases.update(_ + 1).as(true)
    override def refresh(receipt: Receipt, ttl: Duration): IO[LockError, Refreshed]               =
      refreshes
        .updateAndGet(_ :+ ttl)
        .tap(_ => renewed.succeed(()))
        .map: asked =>
          if asked.length > lostAfter then Refreshed.Lost
          else Refreshed.Renewed(Instant.EPOCH, granting.getOrElse(ttl))

  private def fake(
    granted: Option[Hold],
    lostAfter: Int = Int.MaxValue,
    granting: Option[Duration] = None,
  ) =
    for
      refreshes <- Ref.make(Chunk.empty[Duration])
      releases  <- Ref.make(0)
      renewed   <- Promise.make[Nothing, Unit]
    yield (Fake(granted, refreshes, releases, renewed, lostAfter, granting), refreshes, releases, renewed)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("DistributedLock")(
    test("no grant means the effect never runs") {
      for
        (client, _, releases, _) <- fake(granted = None)
        ran                      <- Ref.make(false)
        answer                   <- DistributedLock(client).acquire("n", 1.second, 1.second)(ran.set(true))
        started                  <- ran.get
        gaveBack                 <- releases.get
      yield assertTrue(answer.isEmpty, !started, gaveBack == 0)
    },
    test("the effect's answer comes back, and the lock with it") {
      for
        (client, _, releases, _) <- fake(granted = Some(hold))
        answer                   <- DistributedLock(client).acquire("n", 1.second, 1.second)(ZIO.succeed(42))
        gaveBack                 <- releases.get
      yield assertTrue(answer.contains(42), gaveBack == 1)
    },
    test("a failing effect gives the lock back, and its own error is what surfaces") {
      for
        (client, _, releases, _) <- fake(granted = Some(hold))
        outcome                  <- DistributedLock(client).acquire("n", 1.second, 1.second)(ZIO.fail("mine")).either
        gaveBack                 <- releases.get
      yield assertTrue(outcome == Left("mine"), gaveBack == 1)
    },
    test("an interrupted effect gives the lock back") {
      for
        (client, _, releases, _) <- fake(granted = Some(hold))
        inside                   <- Promise.make[Nothing, Unit]
        fiber                    <- DistributedLock(client).acquire("n", 1.second, 1.second)(inside.succeed(()) *> ZIO.never).fork
        _                        <- inside.await
        _                        <- fiber.interrupt
        gaveBack                 <- releases.get.repeatUntil(_ == 1)
      yield assertTrue(gaveBack == 1)
    },
    test("the lock goes back after the effect, not before it") {
      // Counting releases is not enough: a release that fires early still counts once, and leaves the
      // effect running unlocked. What matters is the order.
      for
        (client, _, releases, _) <- fake(granted = Some(hold))
        heldDuringEffect         <- Ref.make(false)
        answer                   <- DistributedLock(client).acquire("n", 1.second, 1.second) {
                                      releases.get.map(_ == 0).flatMap(heldDuringEffect.set)
                                    }
        stillHeld                <- heldDuringEffect.get
        gaveBack                 <- releases.get
      yield assertTrue(stillHeld, gaveBack == 1, answer.isDefined)
    },
    test("the lease is pushed forward while the effect runs") {
      for
        (client, refreshes, releases, renewed) <- fake(granted = Some(hold))
        finish                                 <- Promise.make[Nothing, Unit]
        fiber                                  <- DistributedLock(client).acquire("n", lease, 1.second)(finish.await).fork
        _                                      <- renewed.await // a renewal happened; no guess about when
        _                                      <- finish.succeed(())
        _                                      <- fiber.join
        pushed                                 <- refreshes.get
        back                                   <- releases.get
      yield assertTrue(pushed.nonEmpty, back == 1)
    },
    test("the renewals follow the lease the service granted, not the one that was asked for") {
      // The service clamps a ttl it considers too long and says so in the grant, so a caller asking for an
      // hour here holds a 40-millisecond lease. A renewal arriving at all is what separates the two: on
      // the asked-for hour, the first one falls due long after this test has finished.
      for
        (client, refreshes, _, renewed) <- fake(granted = Some(hold))
        finish                          <- Promise.make[Nothing, Unit]
        fiber                           <- DistributedLock(client).acquire("n", 1.hour, 1.second)(finish.await).fork
        pushed                          <- renewed.await.timeout(5.seconds)
        _                               <- finish.succeed(())
        _                               <- fiber.join
        asked                           <- refreshes.get
      yield assertTrue(pushed.isDefined, asked.nonEmpty, asked.forall(_ == lease))
    },
    test("a lease that comes back shorter retimes the renewal after it") {
      // A ceiling that moves under a running hold: the service grants less than it was asked for, so the
      // renewal after it asks for — and is timed by — what the last one actually got.
      for
        (client, refreshes, _, _) <- fake(granted = Some(hold), granting = Some(15.millis))
        finish                    <- Promise.make[Nothing, Unit]
        fiber                     <- DistributedLock(client).acquire("n", 1.second, 1.second)(finish.await).fork
        asked                     <- refreshes.get.repeatUntil(_.length >= 2)
        _                         <- finish.succeed(())
        _                         <- fiber.join
      yield assertTrue(asked.take(2) == Chunk(lease, 15.millis))
    },
    test("a lost hold stops the renewals and leaves the effect running") {
      // The decision this form makes: it does not interrupt, because that would promise an exclusion the
      // service does not give. A caller needing more than best effort takes the fence from LockClient.
      for
        (client, refreshes, releases, renewed) <- fake(granted = Some(hold), lostAfter = 0)
        finish                                 <- Promise.make[Nothing, Unit]
        fiber                                  <- DistributedLock(client).acquire("n", lease, 1.second)(finish.await.as("finished")).fork
        _                                      <- renewed.await // the first renewal answered Lost, so the loop is over
        _                                      <- finish.succeed(())
        answer                                 <- fiber.join
        pushed                                 <- refreshes.get
        back                                   <- releases.get
      yield assertTrue(answer.contains("finished"), pushed.length == 1, back == 1)
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)
