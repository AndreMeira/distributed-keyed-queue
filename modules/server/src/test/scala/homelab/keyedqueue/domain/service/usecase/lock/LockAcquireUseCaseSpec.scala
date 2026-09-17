package homelab.keyedqueue.domain.service.usecase.lock


import homelab.keyedqueue.SpecHelper
import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.domain.service.persistence.LockStore
import homelab.keyedqueue.domain.types.LockName
import homelab.keyedqueue.infrastructure.redis.RedisSpecSupport
import zio.*
import zio.test.*


/**
 * Waiting for a lock: enter, hold a ticket, park until the next known event, ask again.
 *
 * These run against a real store because what they assert is what the ticket ordering and the waiting do
 * together — a grant that follows arrival order is half Lua and half loop, and neither half shows it alone.
 */
object LockAcquireUseCaseSpec extends ZIOSpecDefault:

  def spec: Spec[TestEnvironment & Scope, Any] = {
    suite("LockAcquireUseCase")(
      test("a blocked acquire completes when the holder releases") {
        for
          acquire   <- ZIO.service[LockAcquireUseCase]
          store     <- ZIO.service[LockStore]
          held      <- store.tryAcquire(LockName("e"), 30.seconds).someOrFailException
          waiter    <- acquire(Helper.acquiring("e", 30.seconds, 5.seconds)).fork
          _         <- ZIO.sleep(300.millis)
          parked    <- waiter.poll.map(_.isEmpty)
          _         <- store.release(held.claim)
          recovered <- waiter.join
        yield assertTrue(parked, Helper.granted(recovered))
      },
      test("a release reaches a parked waiter through the wake path, not a poll") {
        // The waiter has no in-process signal from the release: it parks on its mailbox, and what wakes it
        // is the entry release.lua appends to the wake stream. A patience with headroom over the round
        // trip, so acquiring by timing out would fail the assertion instead of passing it.
        for
          acquire           <- ZIO.service[LockAcquireUseCase]
          store             <- ZIO.service[LockStore]
          held              <- store.tryAcquire(LockName("cross"), 30.seconds).someOrFailException
          waiter            <- acquire(Helper.acquiring("cross", 30.seconds, 10.seconds)).timed.fork
          _                 <- ZIO.sleep(300.millis)
          parked            <- waiter.poll.map(_.isEmpty)
          _                 <- store.release(held.claim)
          (took, recovered) <- waiter.join
        yield assertTrue(parked, Helper.granted(recovered), took < 5.seconds)
      },
      test("a waiter parked on a dead holder reclaims at lease expiry — no release, no trim, no polling") {
        // The deadline-aware wait: the enter is refused with "re-check when the lease ends", so the waiter
        // wakes itself at exactly the moment the lock becomes reclaimable, with no wake ever arriving.
        for
          acquire           <- ZIO.service[LockAcquireUseCase]
          store             <- ZIO.service[LockStore]
          _                 <- store.tryAcquire(LockName("h"), 500.millis).someOrFailException
          waiter            <- acquire(Helper.acquiring("h", 30.seconds, 10.seconds)).timed.fork
          _                 <- ZIO.sleep(200.millis)
          parked            <- waiter.poll.map(_.isEmpty)
          (took, recovered) <- waiter.join
        yield assertTrue(parked, Helper.granted(recovered), took < 2.seconds)
      },
      test("grants follow arrival order — first to enter is first granted") {
        for
          acquire <- ZIO.service[LockAcquireUseCase]
          store   <- ZIO.service[LockStore]
          held    <- store.tryAcquire(LockName("fifo"), 30.seconds).someOrFailException
          order   <- Ref.make(Chunk.empty[Int])
          turn     = (i: Int) =>
                       acquire(Helper.acquiring("fifo", 5.seconds, 20.seconds)).flatMap: answer =>
                         ZIO.foreachDiscard(Helper.heldBy(answer))(claim => order.update(_ :+ i) *> store.release(claim).unit)
          first   <- turn(1).fork
          _       <- ZIO.sleep(200.millis)
          second  <- turn(2).fork
          _       <- ZIO.sleep(200.millis)
          third   <- turn(3).fork
          _       <- ZIO.sleep(200.millis)
          _       <- store.release(held.claim)
          _       <- first.join *> second.join *> third.join
          served  <- order.get
        yield assertTrue(served == Chunk(1, 2, 3))
      },
      test("a newcomer cannot barge past a waiter: tryAcquire never wins through a handover") {
        for
          acquire <- ZIO.service[LockAcquireUseCase]
          store   <- ZIO.service[LockStore]
          held    <- store.tryAcquire(LockName("barge"), 30.seconds).someOrFailException
          waiter  <- acquire(Helper.acquiring("barge", 30.seconds, 10.seconds)).fork
          _       <- ZIO.sleep(300.millis)
          barged  <- Ref.make(false)
          newcomer = store.tryAcquire(LockName("barge"), 30.seconds).flatMap(r => barged.set(true).when(r.isDefined))
          spam    <- newcomer.repeat(Schedule.spaced(5.millis)).fork
          _       <- store.release(held.claim)
          granted <- waiter.join
          _       <- ZIO.sleep(300.millis)
          _       <- spam.interrupt
          sneaked <- barged.get
        yield assertTrue(Helper.granted(granted), !sneaked)
      },
      test("a waiter interrupted mid-wait gives up its place, so the next one is not delayed") {
        // What the withdrawal finaliser is for. A queues, B queues behind it, then A is interrupted. If A's
        // ticket stayed at the head, B would wait out A's patience — thirty seconds — instead of taking the
        // lock as soon as the holder releases.
        for
          acquire       <- ZIO.service[LockAcquireUseCase]
          store         <- ZIO.service[LockStore]
          held          <- store.tryAcquire(LockName("abandoned"), 30.seconds).someOrFailException
          first         <- acquire(Helper.acquiring("abandoned", 5.seconds, 30.seconds)).fork
          _             <- ZIO.sleep(300.millis)
          second        <- acquire(Helper.acquiring("abandoned", 5.seconds, 30.seconds)).timed.fork
          _             <- ZIO.sleep(300.millis)
          _             <- first.interrupt
          _             <- store.release(held.claim)
          outcome       <- second.join
          (took, answer) = outcome
        yield assertTrue(Helper.granted(answer), took < 5.seconds)
      },
      test("mutual exclusion under contention: never two holders at once") {
        val fibers   = 12
        val perFiber = 2
        for
          acquire  <- ZIO.service[LockAcquireUseCase]
          store    <- ZIO.service[LockStore]
          inside   <- Ref.make(0)
          breach   <- Ref.make(false)
          worker    = ZIO.foreachDiscard(1 to perFiber): _ =>
                        ZIO.acquireReleaseWith(
                          acquire(Helper.acquiring("f", 30.seconds, 30.seconds))
                        )(answer => ZIO.foreachDiscard(Helper.heldBy(answer))(claim => store.release(claim).ignore)): answer =>
                          ZIO
                            .when(Helper.granted(answer)):
                              for
                                n <- inside.updateAndGet(_ + 1)
                                _ <- breach.set(true).when(n != 1) // anyone else inside == exclusion broken
                                _ <- ZIO.sleep(3.millis)           // widen the window an overlap would use
                                _ <- inside.update(_ - 1)
                              yield ()
                            .unit
          _        <- ZIO.foreachParDiscard(1 to fibers)(_ => worker)
          broken   <- breach.get
          leftover <- inside.get
        yield assertTrue(!broken, leftover == 0)
      },
    ) @@ RedisSpecSupport.Aspect.init @@ SpecHelper.Aspect.common
  }.provideSomeShared[Scope](
    RedisSpecSupport.config(30.seconds) >+> RedisSpecSupport.layer >+> UseCaseSpecSupport.layer
  )
