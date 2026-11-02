package homelab.keyedqueue.domain.service.usecase.lock


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.lock.{ Claim, Hold, Position, Turn }
import homelab.keyedqueue.domain.request.lock.{ AcquireRequest, TryAcquireRequest }
import homelab.keyedqueue.domain.response.lock.AcquireResponse
import homelab.keyedqueue.domain.service.persistence.LockStore
import homelab.keyedqueue.domain.service.readiness.LockReadiness
import homelab.keyedqueue.domain.service.validation.LockInputValidation
import homelab.keyedqueue.domain.types.{ LockName, Ticket, Token }
import zio.*
import zio.test.*

import java.time.Instant


/**
 * A lock granted for a caller who has gone: the call was cancelled while the store was granting. The hold
 * cannot reach anyone, so it has to be released rather than block every other caller until its lease runs out.
 */
object LockHandBackSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment, Any] =
    suite("lock hand-back")(
      test("an acquire cancelled while its first ask is granting releases the hold at once") {
        for
          store <- Recording.make(placing = Answer.Granting, asking = Answer.Granting)
          exit  <- cancelledDuring(store, acquire(store))
          freed <- store.released
        yield assertTrue(exit.isInterrupted, freed == Chunk(hold.claim))
      } @@ TestAspect.nonFlaky(50),
      test("an acquire cancelled while a queued ask is granting releases the hold at once") {
        for
          store <- Recording.make(placing = Answer.Queueing, asking = Answer.Granting)
          exit  <- cancelledDuring(store, acquire(store))
          freed <- store.released
        yield assertTrue(exit.isInterrupted, freed == Chunk(hold.claim))
      } @@ TestAspect.nonFlaky(50),
      test("a try-acquire cancelled while it is granting releases the hold at once") {
        for
          store <- Recording.make(placing = Answer.Granting, asking = Answer.Granting)
          exit  <- cancelledDuring(store, tryAcquire(store))
          freed <- store.released
        yield assertTrue(exit.isInterrupted, freed == Chunk(hold.claim))
      } @@ TestAspect.nonFlaky(50),
      test("a grant that reaches its caller is not released") {
        for
          store   <- Recording.make(placing = Answer.Queueing, asking = Answer.Granting)
          _       <- store.open
          granted <- acquire(store)
          tried   <- tryAcquire(store)
          freed   <- store.released
        yield assertTrue(freed.isEmpty, granted == answered, tried == answered)
      },
    ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)

  private object Support:

    /** What the store answers a `place` or an `ask` with. */
    enum Answer:
      case Granting, Queueing

    val hold: Hold = Hold(Claim(LockName("job"), Token(1L)), Instant.EPOCH)

    /** What a caller is told when it gets [[hold]] for the ttl these requests ask. */
    val answered: AcquireResponse = AcquireResponse.Granted(hold.claim.reference, hold.claim.token, hold.leaseUntil, 30.seconds)

    /**
     * Run `call`, cancel it while the store is between granting and answering, then let the store answer.
     *
     * @param store the store whose grant is held open
     * @param call the use-case call to cancel
     * @tparam A what the call answers with
     * @return how the call ended
     */
    def cancelledDuring[A](store: Recording, call: IO[ApplicationError, A]): UIO[Exit[ApplicationError, A]] =
      for
        caller <- call.fork
        _      <- store.granting
        self   <- ZIO.fiberId
        _      <- caller.interruptAsFork(self)
        _      <- store.open
        exit   <- caller.await
      yield exit

    /**
     * A blocking acquire over `store`, with the real readiness.
     *
     * @param store what the use case takes the lock from
     * @return the answer; aborts with `ApplicationError` when the store fails, which this store never does
     */
    def acquire(store: LockStore): IO[ApplicationError, AcquireResponse] =
      LockReadiness.make.flatMap: readiness =>
        LockAcquireUseCase(store, validation, readiness)(AcquireRequest("job", 30.seconds, 5.seconds))

    /**
     * A try-acquire over `store`.
     *
     * @param store what the use case takes the lock from
     * @return the answer; aborts with `ApplicationError` when the store fails, which this store never does
     */
    def tryAcquire(store: LockStore): IO[ApplicationError, AcquireResponse] =
      LockTryAcquireUseCase(store, validation)(TryAcquireRequest("job", 30.seconds))

    val validation: LockInputValidation = LockInputValidation(UseCaseSpecSupport.limits)

    /**
     * A store that grants [[hold]], signals when it has granted, and answers only once it is opened. It records
     * what it was asked to release.
     *
     * @param placing what a `place` answers with
     * @param asking what an `ask` answers with
     * @param granted completed once the store has made a grant
     * @param gate what a grant waits for before answering
     * @param releases what releases have been asked for
     */
    final class Recording(
      placing: Answer,
      asking: Answer,
      granted: Promise[Nothing, Unit],
      gate: Promise[Nothing, Unit],
      releases: Ref[Chunk[Claim]],
    ) extends LockStore:

      /**
       * Wait until the store has made a grant it has not answered with yet.
       *
       * @return noop once it has
       */
      def granting: UIO[Unit] = granted.await

      /**
       * Let every grant answer, now and from here on.
       *
       * @return noop
       */
      def open: UIO[Unit] = gate.succeed(()).unit

      /**
       * What releases have been asked for.
       *
       * @return the claims, oldest first
       */
      def released: UIO[Chunk[Claim]] = releases.get

      /**
       * Grant [[hold]], then wait for the gate before answering.
       *
       * @return the hold
       */
      private def grant: UIO[Hold] = granted.succeed(()) *> gate.await.as(hold)

      override def place(name: LockName, ttl: Duration, within: Duration): IO[ApplicationError.AdapterError, Position] =
        placing match
          case Answer.Granting => grant.map(Position.Granted(_))
          case Answer.Queueing => ZIO.succeed(Position.Queued(Ticket(1L), Duration.Zero))

      override def ask(name: LockName, ttl: Duration, ticket: Ticket): IO[ApplicationError.AdapterError, Turn] =
        asking match
          case Answer.Granting => grant.map(Turn.Granted(_))
          case Answer.Queueing => ZIO.succeed(Turn.Wait(10.millis))

      override def tryAcquire(name: LockName, ttl: Duration): IO[ApplicationError.AdapterError, Option[Hold]] =
        grant.map(Some(_))

      override def release(claim: Claim): IO[ApplicationError.AdapterError, Boolean] =
        releases.update(_ :+ claim).as(true)

      override def withdraw(name: LockName, ticket: Ticket): IO[ApplicationError.AdapterError, Unit] = ZIO.unit

      override def refresh(claim: Claim, ttl: Duration): IO[ApplicationError.AdapterError, (Instant, Boolean)] =
        ZIO.succeed(Instant.EPOCH -> true)

      override def trim(grace: Duration, limit: Int): IO[ApplicationError.AdapterError, Chunk[LockName]] =
        ZIO.succeed(Chunk.empty)

    object Recording:

      /**
       * A store with its gate closed.
       *
       * @param placing what a `place` answers with
       * @param asking what an `ask` answers with
       * @return the store
       */
      def make(placing: Answer, asking: Answer): UIO[Recording] =
        for
          granted  <- Promise.make[Nothing, Unit]
          gate     <- Promise.make[Nothing, Unit]
          releases <- Ref.make(Chunk.empty[Claim])
        yield Recording(placing, asking, granted, gate, releases)
