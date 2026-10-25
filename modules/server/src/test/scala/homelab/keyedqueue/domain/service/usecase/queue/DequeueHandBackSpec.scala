package homelab.keyedqueue.domain.service.usecase.queue


import homelab.common.error.ApplicationError
import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.domain.model.queue.{ Claim, Grant, Settlement, Submission }
import homelab.keyedqueue.domain.model.queue.Settlement.{ Outcome, Verdict }
import homelab.keyedqueue.domain.request.queue.DequeueRequest
import homelab.keyedqueue.domain.response.queue.DequeueResponse
import homelab.keyedqueue.domain.service.maintenance.Watchdog
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.service.readiness.QueueReadiness
import homelab.keyedqueue.domain.service.validation.QueueInputValidation
import homelab.keyedqueue.domain.types.QueueName
import zio.*
import zio.test.*

import java.time.Instant


/**
 * A claim made for a caller who has gone: the call was cancelled while the store was claiming for it. The
 * grant cannot reach anyone, so it has to go back to the store rather than sit claimed until its lease runs
 * out.
 */
object DequeueHandBackSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment, Any] =
    suite("DequeueUseCase hand-back")(
      test("a claim made for a caller cancelled during the claim is handed back at once") {
        for
          entered <- Promise.make[Nothing, Unit]
          gate    <- Promise.make[Nothing, Unit]
          store   <- Recording.make(during = entered.succeed(()) *> gate.await)
          grant    = Helper.grant("orders", "k1", "a")
          _       <- store.hasWork(grant)
          dequeue <- useCase(store)
          caller  <- dequeue(DequeueRequest("orders", 5.seconds, 1)).fork
          _       <- entered.await
          self    <- ZIO.fiberId
          _       <- caller.interruptAsFork(self)
          _       <- gate.succeed(())
          exit    <- caller.await
          settled <- store.settled
        yield assertTrue(exit.isInterrupted, settled == Chunk(handedBack(grant)))
      } @@ TestAspect.nonFlaky(50),
      test("a claim for a caller still waiting is granted, and nothing is handed back") {
        for
          store   <- Recording.make(during = ZIO.unit)
          grant    = Helper.grant("orders", "k1", "a")
          _       <- store.hasWork(grant)
          dequeue <- useCase(store)
          answer  <- dequeue(DequeueRequest("orders", 5.seconds, 1))
          settled <- store.settled
        yield assertTrue(answer == DequeueResponse.fromGrant(grant), settled.isEmpty)
      },
      test("whatever the interleaving, a caller cancelled before its claim answers gets it handed back") {
        for
          rounds  <- ZIO.foreach(Chunk.fromIterable(1 to 2000))(round)
          stranded = rounds.count(_ == Round.Stranded)
        yield assertTrue(stranded == 0, rounds.forall(_ == Round.HandedBack))
      },
    ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)

  private object Support:

    /** What became of the one grant a round offered. */
    enum Round:
      case Delivered, HandedBack, Unclaimed, Stranded

    /**
     * One caller whose claim is held open while the interrupt is requested, with random pauses on both sides
     * so the interleavings vary, and what became of the grant.
     *
     * @param index which round, which names the key so rounds cannot be confused
     * @return what became of the grant
     */
    def round(index: Int): UIO[Round] =
      for
        entered  <- Promise.make[Nothing, Unit]
        gate     <- Promise.make[Nothing, Unit]
        store    <- Recording.make(during = entered.succeed(()) *> jitter *> gate.await)
        grant     = Helper.grant("orders", s"k$index", "a")
        _        <- store.hasWork(grant)
        dequeue  <- useCase(store)
        caller   <- dequeue(DequeueRequest("orders", 5.seconds, 1)).fork
        _        <- entered.await *> jitter
        self     <- ZIO.fiberId
        _        <- caller.interruptAsFork(self)
        _        <- jitter *> gate.succeed(())
        exit     <- caller.await
        claimed  <- store.claimed
        settled  <- store.settled
        delivered = exit match
                      case Exit.Success(answer) => answer == DequeueResponse.fromGrant(grant)
                      case Exit.Failure(_)      => false
      yield
        if delivered then Round.Delivered
        else if settled == Chunk(handedBack(grant)) then Round.HandedBack
        else if claimed.isEmpty then Round.Unclaimed
        else Round.Stranded

    /**
     * A random pause, in yields rather than time, so the moments it lands on vary without slowing a round.
     *
     * @return noop after up to 30 yields
     */
    def jitter: UIO[Unit] = Random.nextIntBounded(30).flatMap(yields => ZIO.yieldNow.repeatN(yields)).unit

    /**
     * The use case over `store`, with the real readiness and limits no request here reaches.
     *
     * @param store what the use case claims from
     * @return the use case
     */
    def useCase(store: QueueStore): UIO[DequeueUseCase] =
      for
        readiness <- QueueReadiness.make
        watchdog  <- Watchdog.make(store, UseCaseSpecSupport.sweeping)
      yield DequeueUseCase(store, watchdog, QueueInputValidation(UseCaseSpecSupport.limits), readiness)

    /**
     * The settlement a hand-back of `grant` makes.
     *
     * @param grant what was claimed
     * @return every message failed, with no backoff
     */
    def handedBack(grant: Grant): Settlement =
      Settlement(grant.claim, grant.messages.map(owned => Outcome(owned.id, Verdict.Failed)), retryAfter = None)

    /**
     * A store that hands out the grants it was given, runs `during` after taking one and before answering,
     * and records what it claimed and what it was asked to settle.
     *
     * @param pending the grants still to be handed out
     * @param claims what claims have taken
     * @param settlements what settles have been asked for
     * @param during what a claim does between taking a grant and answering with it
     */
    final class Recording(
      pending: Ref[Chunk[Grant]],
      claims: Ref[Chunk[Grant]],
      settlements: Ref[Chunk[Settlement]],
      during: UIO[Unit],
    ) extends QueueStore:

      /**
       * Make a grant available to the next claim.
       *
       * @param grant what the next claim should find
       * @return noop
       */
      def hasWork(grant: Grant): UIO[Unit] = pending.update(_ :+ grant)

      /**
       * What claims have taken.
       *
       * @return the grants, oldest first
       */
      def claimed: UIO[Chunk[Grant]] = claims.get

      /**
       * What settles have been asked for.
       *
       * @return the settlements, oldest first
       */
      def settled: UIO[Chunk[Settlement]] = settlements.get

      override val leaseTtl: Duration = 30.seconds

      override def attemptClaim(queue: QueueName, batch: Int): IO[ApplicationError.AdapterError, Option[Grant]] =
        for
          found <- pending.modify(queued => (queued.headOption, queued.drop(1)))
          _     <- claims.update(_ ++ Chunk.fromIterable(found))
          _     <- during
        yield found

      override def settle(settlement: Settlement): IO[ApplicationError.AdapterError, Boolean] =
        settlements.update(_ :+ settlement).as(true)

      override def enqueue(submission: Submission): IO[ApplicationError.AdapterError, Long] = ZIO.succeed(1L)

      override def renew(claims: Chunk[Claim]): IO[ApplicationError.AdapterError, (Instant, Chunk[Claim])] =
        ZIO.succeed(Instant.EPOCH -> Chunk.empty)

      override def sweep(queue: QueueName, limit: Int): IO[ApplicationError.AdapterError, QueueStore.Swept] =
        ZIO.succeed(QueueStore.Swept(Chunk.empty, Chunk.empty))

    object Recording:

      /**
       * A store with nothing pending.
       *
       * @param during what each claim does between taking a grant and answering with it
       * @return the store
       */
      def make(during: UIO[Unit]): UIO[Recording] =
        for
          pending     <- Ref.make(Chunk.empty[Grant])
          claims      <- Ref.make(Chunk.empty[Grant])
          settlements <- Ref.make(Chunk.empty[Settlement])
        yield Recording(pending, claims, settlements, during)
