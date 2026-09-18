package homelab.keyedqueue.domain.service.usecase.lock


import homelab.keyedqueue.SpecHelper
import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.domain.request.lock.TryAcquireRequest
import homelab.keyedqueue.domain.service.persistence.LockStore
import homelab.keyedqueue.domain.types.LockName
import homelab.keyedqueue.infrastructure.redis.RedisSpecSupport
import zio.*
import zio.test.*


/**
 * Taking a lock without waiting for it: granted when nothing stands in the way, refused otherwise.
 *
 * These run against a real store because the refusal that matters is the one no client can see coming — a
 * free lock somebody is already queued for, which only the ticket list knows about.
 */
object LockTryAcquireUseCaseSpec extends ZIOSpecDefault:

  private def asking(name: String) = TryAcquireRequest(name, 30.seconds)

  def spec: Spec[TestEnvironment & Scope, Any] = {
    suite("LockTryAcquireUseCase")(
      test("a free lock is taken") {
        for
          tryAcquire <- ZIO.service[LockTryAcquireUseCase]
          answer     <- tryAcquire(asking("free"))
        yield assertTrue(Helper.granted(answer))
      },
      test("a held lock is refused rather than waited for") {
        for
          tryAcquire <- ZIO.service[LockTryAcquireUseCase]
          store      <- ZIO.service[LockStore]
          _          <- store.tryAcquire(LockName("taken"), 30.seconds).someOrFailException
          answer     <- tryAcquire(asking("taken"))
        yield assertTrue(!Helper.granted(answer))
      },
      test("a free lock somebody is queued for is refused: taking it would be barging") {
        // The refusal a caller cannot predict. The holder is gone, so the lock is free — but a waiter is
        // ahead in the ticket list, and the whole point of the tickets is that arrival order decides.
        for
          tryAcquire <- ZIO.service[LockTryAcquireUseCase]
          store      <- ZIO.service[LockStore]
          held       <- store.tryAcquire(LockName("queued"), 30.seconds).someOrFailException
          _          <- store.place(LockName("queued"), 30.seconds, 30.seconds)
          _          <- store.release(held.claim)
          answer     <- tryAcquire(asking("queued"))
        yield assertTrue(!Helper.granted(answer))
      },
      test("a grant carries a receipt that releases it") {
        for
          tryAcquire <- ZIO.service[LockTryAcquireUseCase]
          store      <- ZIO.service[LockStore]
          answer     <- tryAcquire(asking("receipted"))
          claim       = Helper.heldBy(answer)
          released   <- ZIO.foreach(claim)(store.release)
        yield assertTrue(Helper.granted(answer), released.contains(true))
      },
    ) @@ RedisSpecSupport.Aspect.init @@ SpecHelper.Aspect.common
  }.provideSomeShared[Scope](
    RedisSpecSupport.config(30.seconds) >+> RedisSpecSupport.layer >+> UseCaseSpecSupport.tryAcquireLayer
  )
