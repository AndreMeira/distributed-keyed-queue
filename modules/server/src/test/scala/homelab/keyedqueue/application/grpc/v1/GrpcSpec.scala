package homelab.keyedqueue.application.grpc.v1


import com.google.protobuf.duration.Duration as ProtoDuration
import homelab.keyedqueue.SpecHelper
import homelab.keyedqueue.SpecHelper.Helper
import homelab.keyedqueue.domain.types.LockName
import homelab.keyedqueue.infrastructure.redis.Connection
import homelab.keyedqueue.infrastructure.redis.Module as RedisModule
import homelab.keyedqueue.infrastructure.redis.keys.KeyLayout
import homelab.keyedqueue.v1.*
import homelab.keyedqueue.v1.ZioKeyedLockService.KeyedLockClient
import homelab.keyedqueue.v1.ZioKeyedQueueService.KeyedQueueClient
import io.grpc.{ Status, StatusException }
import zio.*
import zio.test.*


/**
 * The API a consumer actually sees: four calls, over a real server, against a real substrate.
 *
 * The store spec proves the guarantees; this proves the wiring — that a client can enqueue, block for work,
 * settle it, and be told what it still holds, with no knowledge of keys, leases or Redis.
 */
object GrpcSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = {
    suite("KeyedQueue over gRPC")(
      test("the lock API over the network: acquire, refuse, release, re-acquire, fence, refresh") {
        for
          lock    <- ZIO.service[KeyedLockClient]
          secs     = (n: Int) => Some(ProtoDuration(n.toLong))
          first   <- lock.acquire(AcquireRequest("job", secs(30), secs(1)))
          // held now, so a no-wait-ish second acquire returns not-acquired
          again   <- lock.acquire(AcquireRequest("job", secs(30), secs(1)))
          _       <- lock.release(ReleaseRequest(first.receipt))
          // re-acquirable after release, and the fence advanced
          retaken <- lock.acquire(AcquireRequest("job", secs(30), secs(1)))
          renewed <- lock.refresh(RefreshRequest(retaken.receipt, secs(30)))
        yield assertTrue(
          first.acquired,
          first.fence > 0L,
          !again.acquired,
          retaken.acquired,
          retaken.fence > first.fence, // fenced: a fresh grant is a strictly higher token
          renewed.renewed,
        )
      },
      test("try-acquire over the network: taken when free, refused when held, refused when queued for") {
        // The verb that says no instead of waiting, over the network. `barging` asks *after* the holder let
        // go, and only once the waiter's ticket is in Redis, so a ticket is there to refuse it. What refuses
        // it is whichever came first: the ticket still queued, or that same waiter having already won. Which
        // one is a race, so this asserts only that it was refused — the queued-and-still-refused case is
        // pinned deterministically in LockTryAcquireUseCaseSpec, where a ticket can be placed with nobody
        // racing to claim it.
        for
          lock     <- ZIO.service[KeyedLockClient]
          secs      = (n: Int) => Some(ProtoDuration(n.toLong))
          taken    <- lock.tryAcquire(TryAcquireRequest("try", secs(30)))
          refused  <- lock.tryAcquire(TryAcquireRequest("try", secs(30)))
          queueing <- lock.acquire(AcquireRequest("try", secs(30), secs(5))).fork
          // Bounded by the acquire's patience: once that has run out, the ticket can no longer appear.
          observed <- queued(LockName("try")).timeout(5.seconds).map(_.isDefined)
          _        <- lock.release(ReleaseRequest(taken.receipt))
          barging  <- lock.tryAcquire(TryAcquireRequest("try", secs(30)))
          waited   <- queueing.join
          noName   <- lock.tryAcquire(TryAcquireRequest("", secs(30))).exit
        yield assertTrue(
          observed,          // the waiter's ticket reached Redis before the release
          taken.acquired,
          taken.fence > 0L,
          !refused.acquired, // held
          !barging.acquired, // no longer held, and still not this caller's to take
          waited.acquired,   // the ticket is what got it
          noName.isFailure,
        )
      },
      test("enqueue, dequeue, settle — the loop a consumer writes") {
        for
          client  <- ZIO.service[KeyedQueueClient]
          _       <- client.enqueue(EnqueueRequest("jobs", Some(Helper.protoMessage("k1", "hello"))))
          reply   <- client.dequeue(DequeueRequest("jobs", Some(ProtoDuration(seconds = 2))))
          settled <- client.settle(Helper.settle(reply, Outcome.OUTCOME_DONE))
          empty   <- client.dequeue(DequeueRequest("jobs", Some(ProtoDuration(seconds = 1))))
        yield assertTrue(
          Helper.bodies(reply) == Seq("hello"),
          reply.head.map(_.attempt).contains(1),
          reply.receipt.nonEmpty,
          reply.leaseExpiresAt.isDefined,
          settled.applied == Applied.APPLIED_OK,
          empty.head.isEmpty, // the queue is drained, and a timeout is an empty response, not an error
        )
      },
      test("a claim hands over a batch, and each message is settled by name") {
        for
          client  <- ZIO.service[KeyedQueueClient]
          _       <- ZIO.foreachDiscard(1 to 4)(n => client.enqueue(EnqueueRequest("batch", Some(Helper.protoMessage("k1", s"v$n")))))
          reply   <- client.dequeue(DequeueRequest("batch", Some(ProtoDuration(seconds = 2)), maxBatch = 3))
          settled <- client.settle(Helper.settle(reply, Outcome.OUTCOME_DONE))
          next    <- client.dequeue(DequeueRequest("batch", Some(ProtoDuration(seconds = 2))))
        yield assertTrue(
          Helper.bodies(reply) == Seq("v1", "v2", "v3"),
          reply.backlogDepth == 3 - 3 + 1, // v4, still queued behind the batch
          settled.applied == Applied.APPLIED_OK,
          Helper.bodies(next) == Seq("v4"),
        )
      },
      test("a consumer can settle part of its batch and keep the rest") {
        // Conflation, and the shape that makes it possible: the consumer sees the whole batch, decides which
        // are superseded, and says so message by message. The key stays its own until nothing is owed.
        for
          client  <- ZIO.service[KeyedQueueClient]
          _       <- ZIO.foreachDiscard(1 to 3)(n => client.enqueue(EnqueueRequest("partial", Some(Helper.protoMessage("k1", s"v$n")))))
          reply   <- client.dequeue(DequeueRequest("partial", Some(ProtoDuration(seconds = 2)), maxBatch = 3))
          ids      = Helper.claimed(reply).map(_.messageId)
          // v1 and v2 are superseded by v3, so they are acknowledged without being worked.
          partial <- client.settle(SettleRequest(reply.receipt, outcomes = ids.take(2).map(Helper.done).toSeq))
          // Still owed v3, so nobody else may have the key.
          blocked <- client.dequeue(DequeueRequest("partial", Some(ProtoDuration(seconds = 1))))
          _       <- client.settle(SettleRequest(reply.receipt, outcomes = ids.drop(2).map(Helper.done).toSeq))
          drained <- client.dequeue(DequeueRequest("partial", Some(ProtoDuration(seconds = 1))))
        yield assertTrue(
          partial.applied == Applied.APPLIED_OK,
          blocked.head.isEmpty, // the claim was still alive
          drained.head.isEmpty, // and by then there was nothing left
        )
      },
      test("a settle replayed with the same receipt is harmless, and one after the claim ends is stale") {
        // What an at-least-once RPC does on a retry. Naming a message the claim no longer owns changes
        // nothing; using a token the claim has finished with is refused.
        for
          client <- ZIO.service[KeyedQueueClient]
          _      <- client.enqueue(EnqueueRequest("replay", Some(Helper.protoMessage("k1", "once"))))
          reply  <- client.dequeue(DequeueRequest("replay", Some(ProtoDuration(seconds = 2))))
          first  <- client.settle(Helper.settle(reply, Outcome.OUTCOME_DONE))
          second <- client.settle(Helper.settle(reply, Outcome.OUTCOME_DONE))
        yield assertTrue(first.applied == Applied.APPLIED_OK, second.applied == Applied.APPLIED_STALE)
      },
      test("heartbeat renews what is held and names what is not") {
        for
          client <- ZIO.service[KeyedQueueClient]
          _      <- client.enqueue(EnqueueRequest("beats", Some(Helper.protoMessage("k1", "work"))))
          reply  <- client.dequeue(DequeueRequest("beats", Some(ProtoDuration(seconds = 2))))
          beat   <- client.heartbeat(HeartbeatRequest(Seq(reply.receipt, "not-a-receipt")))
        yield assertTrue(
          beat.stale == Seq("not-a-receipt"), // the real one was renewed; the nonsense one named
          beat.renewedUntil.isDefined,
        )
      },
      test("a message with no encoding is refused, and one with no key too") {
        for
          client  <- ZIO.service[KeyedQueueClient]
          noCodec <- client.enqueue(EnqueueRequest("bad", Some(Helper.protoMessage("k1", "x").copy(encoding = "")))).exit
          noKey   <- client.enqueue(EnqueueRequest("bad", Some(Helper.protoMessage("", "x")))).exit
        yield assertTrue(noCodec.isFailure, noKey.isFailure)
      },
      test("a request with two problems is refused once, naming both") {
        // Accumulating validation is only worth having if it survives to the caller: one INVALID_ARGUMENT
        // carrying every reason, rather than one round trip per mistake.
        for
          client <- ZIO.service[KeyedQueueClient]
          failed <- client.enqueue(EnqueueRequest("", Some(Helper.protoMessage("", "x")))).flip
          // `flip` gives the error channel, which the stub types as `StatusException`, so there is no other
          // shape to match on. `Option` is for the status itself, which the exception may carry as null.
          status  = Option(failed.getStatus)
        yield assertTrue(
          status.map(_.getCode).contains(Status.Code.INVALID_ARGUMENT),
          status.flatMap(reported => Option(reported.getDescription)).exists(_.contains("a queue name is required")),
          status.flatMap(reported => Option(reported.getDescription)).exists(_.contains("a message key is required")),
        )
      },
      test("a scheduled message is delivered once it falls due, moved by the server's own mover") {
        // End to end inside one server: the schedule is recorded, the timer fires at the due time, the mover
        // appends the message and wakes the queue, and the parked dequeue is handed it.
        for
          client    <- ZIO.service[KeyedQueueClient]
          scheduled <- client.schedule(ScheduleRequest("later", Some(Helper.protoMessage("k1", "soon")), Some(millis(300))))
          started   <- Clock.instant
          reply     <- client.dequeue(DequeueRequest("later", Some(ProtoDuration(seconds = 3))))
          ended     <- Clock.instant
        yield assertTrue(
          scheduled.scheduled,
          scheduled.dueIn.contains(millis(300)),
          Helper.bodies(reply) == Seq("soon"),
          Duration.fromInterval(started, ended) >= 200.millis,
        )
      },
      test("a schedule repeating an id still scheduled answers scheduled = false, with the first one's time") {
        for
          client <- ZIO.service[KeyedQueueClient]
          first  <- client.schedule(ScheduleRequest("once", Some(Helper.protoMessage("k1", "m")), Some(ProtoDuration(seconds = 60))))
          second <- client.schedule(ScheduleRequest("once", Some(Helper.protoMessage("k1", "m")), Some(ProtoDuration(seconds = 1))))
        yield assertTrue(first.scheduled, !second.scheduled, second.dueIn.exists(_.seconds > 1L))
      },
      test("a schedule with no delay, or one beyond the horizon, is refused as INVALID_ARGUMENT with its reason") {
        for
          client  <- ZIO.service[KeyedQueueClient]
          absent  <- client.schedule(ScheduleRequest("refused", Some(Helper.protoMessage("k1", "m")), None)).flip
          tooLate <- client.schedule(ScheduleRequest("refused", Some(Helper.protoMessage("k1", "m")), Some(ProtoDuration(seconds = 8 * 86_400)))).flip
        yield assertTrue(
          invalid(absent).exists(_.contains("deliver_after is required")),
          invalid(tooLate).exists(_.contains("deliver_after cannot exceed")),
        )
      },
      test("a duration out of protobuf's range is refused as INVALID_ARGUMENT, not read as another or thrown on") {
        // Nanos past a second used to carry into the seconds: read as a 3 s wait here, and an overflow
        // answered INTERNAL with seconds near Long.MaxValue (#78).
        for
          client    <- ZIO.service[KeyedQueueClient]
          lock      <- ZIO.service[KeyedLockClient]
          carried   <- client.dequeue(DequeueRequest("refused", Some(ProtoDuration(seconds = 1, nanos = 2_000_000_000)))).flip
          overflows <- client.dequeue(DequeueRequest("refused", Some(ProtoDuration(seconds = Long.MaxValue, nanos = 999_999_999)))).flip
          ttl       <- lock.acquire(AcquireRequest("refused", Some(ProtoDuration(seconds = 30, nanos = -1)), Some(ProtoDuration(seconds = 1)))).flip
        yield assertTrue(
          invalid(carried).exists(_.contains("maxWait: a duration needs seconds within")),
          invalid(overflows).exists(_.contains("maxWait: a duration needs seconds within")),
          invalid(ttl).exists(_.contains("ttl: a duration needs seconds within")),
        )
      },
      test("a schedule with no message cannot be decoded, and is refused as INVALID_ARGUMENT with its reason") {
        for
          client <- ZIO.service[KeyedQueueClient]
          failed <- client.schedule(ScheduleRequest("refused", None, Some(ProtoDuration(seconds = 1)))).flip
        yield assertTrue(invalid(failed).exists(_.contains("a message is required")))
      },
      test("an enqueue with no message cannot be decoded, and is refused as INVALID_ARGUMENT with its reason") {
        // Refused by the codec, before validation: the domain has no absent message to carry. Matched by its
        // type in `QueueService.status`, since the `DecodingError` marker alone would also cover our own
        // decoding defects, which must stay INTERNAL.
        for
          client <- ZIO.service[KeyedQueueClient]
          failed <- client.enqueue(EnqueueRequest("jobs", None)).flip
          status  = Option(failed.getStatus)
        yield assertTrue(
          status.map(_.getCode).contains(Status.Code.INVALID_ARGUMENT),
          status.flatMap(reported => Option(reported.getDescription)).exists(_.contains("a message is required")),
        )
      },
    ) @@ GrpcSpecSupport.Aspect.init @@ SpecHelper.Aspect.common
  }.provideSomeShared[Scope](
    GrpcSpecSupport.config >+> GrpcSpecSupport.layer >+> RedisModule.layout >+> RedisModule.connection
  )

  private object Support {

    /**
     * A protobuf duration of so many milliseconds.
     *
     * @param value the milliseconds
     * @return the duration
     */
    def millis(value: Int): ProtoDuration =
      ProtoDuration(seconds = value / 1000L, nanos = (value % 1000) * 1_000_000)

    /**
     * The description of a refusal, when it is an `INVALID_ARGUMENT`.
     *
     * @param failed what the call failed with
     * @return the description, or `None` when the status is another one or carries no description
     */
    def invalid(failed: StatusException): Option[String] =
      Option(failed.getStatus).filter(_.getCode == Status.Code.INVALID_ARGUMENT).flatMap(status => Option(status.getDescription))

    /**
     * Wait until somebody holds a ticket for `name`.
     *
     * Read straight from the lock's waiters list: the gRPC API has no call that tells a queued lock from a
     * held one, and a test that releases before the ticket lands is testing a free, unqueued lock instead.
     *
     * @param name the lock
     * @return noop once the list is non-empty; aborts with the client's `Throwable` when Redis cannot be read
     */
    def queued(name: LockName): ZIO[Connection & KeyLayout, Throwable, Unit] =
      for
        connection <- ZIO.service[Connection]
        layout     <- ZIO.service[KeyLayout]
        waiters     = layout.lock(name).waiters(name)
        length      = connection.provide(Connection.use(redis => ZIO.fromCompletionStage(redis.llen(waiters))))
        _          <- (ZIO.sleep(10.millis) *> length).repeatUntil(_ > 0L)
      yield ()
  }
