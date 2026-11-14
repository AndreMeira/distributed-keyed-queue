package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.SpecHelper
import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.domain.model.queue.{ Grant, ScheduledSubmission, Submission }
import homelab.keyedqueue.domain.model.queue.Settlement.Verdict
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.keys.KeyLayout
import zio.*
import zio.test.*


/**
 * A nack's backoff against a real substrate: it holds back its key, nothing makes the key claimable before it
 * ends (an enqueue, a scheduled message falling due), and a release makes it claimable once it has. The waits are
 * real, because the scripts read the store's own clock.
 *
 * The held-back keys are the partition's `delayed`, members naming queue and key: these tests also pin that every
 * script reading it agrees on that member.
 */
object RedisQueueStoreBackoffSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = {
    suite("QueueStore over Redis: backoffs")(
      test("a nack's backoff holds its key back until it ends, then a release makes it claimable") {
        for
          store  <- ZIO.service[QueueStore]
          queue   = QueueName("backoff-ends")
          _      <- store.enqueue(Submission(queue, Helper.message(MessageKey("k1"), "m1")))
          first  <- claimed(store, queue)
          _      <- nack(store, first, 300.millis)
          during <- store.attemptClaim(queue, 1)
          early  <- store.releaseDue(limit)
          _      <- ZIO.sleep(400.millis)
          freed  <- store.releaseDue(limit)
          again  <- store.attemptClaim(queue, 1)
        yield assertTrue(
          during.isEmpty,
          early.released == 0,
          early.nextDueIn.exists(_ <= 300.millis + rounding),
          freed.released == 1,
          again.map(_.messages.head.attempt).contains(2),
        )
      },
      test("an enqueue for a held-back key does not make it claimable before its backoff ends") {
        for
          store  <- ZIO.service[QueueStore]
          queue   = QueueName("backoff-enqueue")
          key     = MessageKey("k1")
          _      <- store.enqueue(Submission(queue, Helper.message(key, "m1")))
          first  <- claimed(store, queue)
          _      <- nack(store, first, 5.seconds)
          _      <- store.enqueue(Submission(queue, Helper.message(key, "m2")))
          during <- store.attemptClaim(queue, 1)
          _      <- release(store, queue)
        yield assertTrue(during.isEmpty)
      },
      test("a scheduled message falling due for a held-back key does not make it claimable before its backoff ends") {
        for
          store  <- ZIO.service[QueueStore]
          queue   = QueueName("backoff-schedule")
          key     = MessageKey("k1")
          _      <- store.enqueue(Submission(queue, Helper.message(key, "m1")))
          first  <- claimed(store, queue)
          _      <- nack(store, first, 5.seconds)
          _      <- store.schedule(ScheduledSubmission(Submission(queue, Helper.message(key, "m2")), 100.millis))
          _      <- ZIO.sleep(200.millis)
          moved  <- store.moveDue(limit)
          during <- store.attemptClaim(queue, 1)
          _      <- release(store, queue)
        yield assertTrue(moved.moved == 1, during.isEmpty)
      },
      test("a queue holding multi-byte characters and a key holding a colon survive the member's encoding") {
        for
          store <- ZIO.service[QueueStore]
          queue  = QueueName("q-é-backoff")
          key    = MessageKey("k:ü")
          _     <- store.enqueue(Submission(queue, Helper.message(key, "m1")))
          first <- claimed(store, queue)
          _     <- nack(store, first, 100.millis)
          _     <- ZIO.sleep(200.millis)
          freed <- store.releaseDue(limit)
          again <- store.attemptClaim(queue, 1)
        yield assertTrue(freed.released == 1, again.map(_.claim.key).contains(key))
      },
      test("a release wakes once per queue it made a key claimable in, not once per key") {
        for
          store  <- ZIO.service[QueueStore]
          first   = QueueName("backoff-wake-a")
          second  = QueueName("backoff-wake-b")
          _      <- held(store, first, "k1", 100.millis)
          _      <- held(store, first, "k2", 100.millis)
          _      <- held(store, second, "k1", 100.millis)
          before <- wakes
          _      <- ZIO.sleep(200.millis)
          freed  <- store.releaseDue(limit)
          after  <- wakes
        yield assertTrue(freed.released == 3, after - before == 2L)
      },
      test("a release that hits its limit says so, and the next one releases the rest") {
        for
          store <- ZIO.service[QueueStore]
          queue  = QueueName("backoff-full")
          _     <- held(store, queue, "k1", 100.millis)
          _     <- held(store, queue, "k2", 100.millis)
          _     <- held(store, queue, "k3", 100.millis)
          _     <- ZIO.sleep(200.millis)
          full  <- store.releaseDue(2)
          rest  <- store.releaseDue(2)
        yield assertTrue(full.released == 2, full.full, rest.released == 1, !rest.full)
      },
    ) @@ RedisSpecSupport.Aspect.init @@ SpecHelper.Aspect.common
  }.provideSomeShared[Scope](RedisSpecSupport.config(10.seconds) >+> RedisSpecSupport.layer)

  private object Support {

    /** The most backoffs one release takes per partition in these tests, unless a test is about the limit. */
    val limit: Int = 100

    /**
     * How much later than its wait a backoff's end may read: the store clock is floored when the nack is recorded
     * and when a release measures what is left, so the two may differ by up to a millisecond.
     */
    val rounding: Duration = 1.milli

    /** The layout these tests read and write under: one partition, so one wake stream. */
    val layout: KeyLayout = KeyLayout.single

    /**
     * Claim one message of a queue, which these tests always expect to find.
     *
     * @param store the store
     * @param queue the queue
     * @return the grant; fails when nothing was claimable
     */
    def claimed(store: QueueStore, queue: QueueName): IO[ApplicationError | String, Grant] =
      store.attemptClaim(queue, 1).someOrFail(s"nothing was claimable in $queue")

    /**
     * Nack everything a grant holds, asking its key to wait.
     *
     * @param store the store
     * @param grant the claim
     * @param wait how long the key should wait
     * @return noop once settled; aborts with the store's failure
     */
    def nack(store: QueueStore, grant: Grant, wait: Duration): IO[ApplicationError, Unit] =
      store.settle(Helper.settlement(grant.claim, grant.messages.map(_.id -> Verdict.Failed), wait)).unit

    /**
     * Enqueue a message for a key, claim it, and nack it with a wait, so the key is held back.
     *
     * @param store the store
     * @param queue the queue
     * @param key the key, which also names the message
     * @param wait how long the key should wait
     * @return noop once held back; fails when the message was not claimable
     */
    def held(store: QueueStore, queue: QueueName, key: String, wait: Duration): IO[ApplicationError | String, Unit] =
      for
        _     <- store.enqueue(Submission(queue, Helper.message(MessageKey(key), s"m-$key")))
        grant <- claimed(store, queue)
        _     <- nack(store, grant, wait)
      yield ()

    /**
     * Wait out a held-back key's backoff and release it, so nothing is left held for a later test.
     *
     * @param store the store
     * @param queue the queue, for the claim that takes what the release made claimable
     * @return noop once released and claimed; aborts with the store's failure
     */
    def release(store: QueueStore, queue: QueueName): IO[ApplicationError, Unit] =
      ZIO.sleep(5.seconds) *> store.releaseDue(limit) *> store.attemptClaim(queue, 10).unit

    /**
     * How many entries the partition's wake stream holds.
     *
     * @return the length; aborts with the client's `Throwable` when Redis cannot be read
     */
    def wakes: ZIO[Connection, Throwable, Long] =
      for
        connection <- ZIO.service[Connection]
        length     <- connection.provide(Connection.use(length))
      yield length

    /**
     * The wake stream's length, read on a connection.
     *
     * @param redis the connection's commands
     * @return the length
     */
    def length(redis: Connection.Commands): Task[Long] =
      ZIO.fromCompletionStage(redis.xlen(layout.wakeStream(KeyLayout.Partition(0)))).map(_.longValue)
  }
