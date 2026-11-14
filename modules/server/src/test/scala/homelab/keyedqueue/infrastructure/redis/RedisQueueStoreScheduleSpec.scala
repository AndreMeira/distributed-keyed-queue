package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.SpecHelper
import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.domain.model.queue.{ ScheduledSubmission, Submission }
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.keys.KeyLayout
import zio.*
import zio.test.*


/**
 * Scheduling against a real substrate: a scheduled message waits outside its key's line until it falls due, then
 * joins it as an enqueue would. The delays are real, because the scripts read the store's own clock.
 *
 * See `docs/research/message-scheduling.md` for the rules these pin: the first schedule of an id wins, an id is
 * compared with the key's queued messages only when moved, and a move wakes once per queue.
 */
object RedisQueueStoreScheduleSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = {
    suite("QueueStore over Redis: scheduling")(
      test("a scheduled message is not claimable before it falls due, and is once moved after") {
        for
          store  <- ZIO.service[QueueStore]
          queue   = QueueName("schedule-due")
          key     = MessageKey("k1")
          taken  <- store.schedule(scheduled(queue, key, "m1", 300.millis))
          early  <- store.moveDue(limit)
          before <- store.attemptClaim(queue, 1)
          _      <- ZIO.sleep(400.millis)
          late   <- store.moveDue(limit)
          after  <- store.attemptClaim(queue, 1)
        yield assertTrue(
          taken == QueueStore.Scheduled(scheduled = true, dueIn = 300.millis),
          early.moved == 0,
          // At most the delay plus the rounding: a due time is recorded with the store clock rounded up, and a move
          // measures what is left with it rounded down, so the two may differ by up to a millisecond, late not early.
          early.nextDueIn.exists(_ <= 300.millis + rounding),
          before.isEmpty,
          late.moved == 1,
          after.map(Helper.body).contains(Chunk("m1")),
        )
      },
      test("a delay with a fraction of a millisecond is rounded up, so a schedule never falls due early") {
        // Truncated, 500 µs would be recorded as no delay at all, and 1.5 ms as 1 ms (S1).
        for
          store <- ZIO.service[QueueStore]
          queue  = QueueName("schedule-rounded-up")
          half  <- store.schedule(scheduled(queue, MessageKey("k1"), "m1", 500.micros))
          more  <- store.schedule(scheduled(queue, MessageKey("k2"), "m2", 1500.micros))
          // Both fall due at once: move them here, so the next test's move finds only its own.
          _     <- ZIO.sleep(50.millis)
          moved <- store.moveDue(limit)
        yield assertTrue(half.dueIn == 1.milli, more.dueIn == 2.millis, moved.moved == 2)
      },
      test("a schedule repeating an id still scheduled is ignored, and answers the first one's remaining time") {
        for
          store  <- ZIO.service[QueueStore]
          queue   = QueueName("schedule-first-wins")
          key     = MessageKey("k1")
          first  <- store.schedule(scheduled(queue, key, "m1", 10.seconds))
          second <- store.schedule(scheduled(queue, key, "m1", 1.second))
        yield assertTrue(
          first.scheduled,
          !second.scheduled,
          second.dueIn > 1.second,
          second.dueIn <= 10.seconds,
        )
      },
      test("a scheduled message whose id is still queued for its key when it falls due is discarded") {
        for
          store   <- ZIO.service[QueueStore]
          queue    = QueueName("schedule-discard")
          key      = MessageKey("k1")
          _       <- store.enqueue(Submission(queue, Helper.message(key, "m1")))
          _       <- store.schedule(scheduled(queue, key, "m1", 100.millis))
          _       <- ZIO.sleep(200.millis)
          moved   <- store.moveDue(limit)
          claimed <- store.attemptClaim(queue, 10)
        yield assertTrue(
          moved.moved == 0,
          moved.discarded == 1,
          claimed.map(Helper.body).contains(Chunk("m1")),
        )
      },
      test("a queue and a key holding colons and multi-byte characters survive the member's encoding") {
        // The member is `<len(queue)>:<queue><len(key)>:<key><id>`: a separator alone would be fooled by these
        // names, and a length counted in characters rather than bytes by the accented ones.
        for
          store   <- ZIO.service[QueueStore]
          queue    = QueueName("q:é:1")
          key      = MessageKey("k:ü:2")
          _       <- store.schedule(scheduled(queue, key, "m:ß", 100.millis))
          _       <- ZIO.sleep(200.millis)
          moved   <- store.moveDue(limit)
          claimed <- store.attemptClaim(queue, 1)
        yield assertTrue(
          moved.moved == 1,
          claimed.map(_.claim.key).contains(key),
          claimed.map(Helper.body).contains(Chunk("m:ß")),
        )
      },
      test("a scheduled message for a claimed key joins its line behind the claim, and waits for it") {
        for
          store  <- ZIO.service[QueueStore]
          queue   = QueueName("schedule-behind-claim")
          key     = MessageKey("k1")
          _      <- store.enqueue(Submission(queue, Helper.message(key, "m1")))
          held   <- store.attemptClaim(queue, 1)
          _      <- store.schedule(scheduled(queue, key, "m2", 100.millis))
          _      <- ZIO.sleep(200.millis)
          moved  <- store.moveDue(limit)
          during <- store.attemptClaim(queue, 1)
          _      <- Helper.ack(store, queue)(held)
          next   <- store.attemptClaim(queue, 1)
        yield assertTrue(
          moved.moved == 1,
          during.isEmpty,
          next.map(Helper.body).contains(Chunk("m2")),
        )
      },
      test("a move wakes once per queue it made a key claimable in, not once per message") {
        for
          store  <- ZIO.service[QueueStore]
          first   = QueueName("schedule-wake-a")
          second  = QueueName("schedule-wake-b")
          _      <- store.schedule(scheduled(first, MessageKey("k1"), "m1", 100.millis))
          _      <- store.schedule(scheduled(first, MessageKey("k2"), "m2", 100.millis))
          _      <- store.schedule(scheduled(second, MessageKey("k1"), "m3", 100.millis))
          before <- wakes
          _      <- ZIO.sleep(200.millis)
          moved  <- store.moveDue(limit)
          after  <- wakes
        yield assertTrue(moved.moved == 3, after - before == 2L)
      },
      test("a move that hits its limit says so, and the next one moves the rest") {
        for
          store <- ZIO.service[QueueStore]
          queue  = QueueName("schedule-full")
          _     <- ZIO.foreachDiscard(Chunk("k1", "k2", "k3"))(due(store, queue))
          _     <- ZIO.sleep(200.millis)
          full  <- store.moveDue(2)
          rest  <- store.moveDue(2)
        yield assertTrue(full.moved == 2, full.full, rest.moved == 1, !rest.full)
      },
      test("a move reports when the next scheduled message falls due") {
        for
          store <- ZIO.service[QueueStore]
          _     <- store.schedule(scheduled(QueueName("schedule-next"), MessageKey("k1"), "m1", 5.seconds))
          moved <- store.moveDue(limit)
        yield assertTrue(moved.nextDueIn.exists(_ <= 5.seconds + rounding), moved.failed == 0)
      },
    ) @@ RedisSpecSupport.Aspect.init @@ SpecHelper.Aspect.common
  }.provideSomeShared[Scope](RedisSpecSupport.config(2.seconds) >+> RedisSpecSupport.layer)

  private object Support {

    /**
     * How much later than its delay a due time may read: the store clock is rounded up when a due time is
     * recorded and down when a move measures what is left, never the other way round.
     */
    val rounding: Duration = 1.milli

    /** The most entries one move takes per partition in these tests, unless a test is about the limit. */
    val limit: Int = 100

    /** The layout these tests read and write under: one partition, so one wake stream. */
    val layout: KeyLayout = KeyLayout.single

    /**
     * A message scheduled for a key.
     *
     * @param queue where it goes
     * @param key its key
     * @param id its id, and its cargo
     * @param delay how long until it is due
     * @return the scheduled submission
     */
    def scheduled(queue: QueueName, key: MessageKey, id: String, delay: Duration): ScheduledSubmission =
      ScheduledSubmission(Submission(queue, Helper.message(key, id)), delay)

    /**
     * Schedule one message for `key`, due shortly.
     *
     * @param store the store
     * @param queue the queue
     * @param key the key, which also names the message
     * @return noop once scheduled; aborts with the store's failure
     */
    def due(store: QueueStore, queue: QueueName)(key: String): IO[ApplicationError, Unit] =
      store.schedule(scheduled(queue, MessageKey(key), s"m-$key", 100.millis)).unit

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
