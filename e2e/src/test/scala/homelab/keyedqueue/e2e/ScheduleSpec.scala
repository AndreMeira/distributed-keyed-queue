package homelab.keyedqueue.e2e


import homelab.keyedqueue.client.queue.QueueClient
import homelab.keyedqueue.client.queue.model.*
import homelab.keyedqueue.client.{ Endpoint, ServiceError }
import zio.*
import zio.test.*


/**
 * Scheduling, as a consumer of the client sees it, against the deployment: a message scheduled on one instance
 * arrives through the other once it falls due, a repeated id is ignored wherever it is sent, a delay beyond the
 * horizon is refused, and a schedule outlives the instance that took it.
 *
 * The deployment's schedule backstop is 5 s (`DKQ_SCHEDULE_BACKSTOP` in the compose files), so the test that kills
 * the accepting instance waits seconds rather than a minute. See `docs/research/message-scheduling.md`.
 */
object ScheduleSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("scheduling, against the deployment")(
      test("a message scheduled on one instance arrives through the other once it falls due, and not before") {
        for
          dkq       <- ZIO.service[Deployment]
          name       = dkq.queue("schedule-arrives")
          a         <- client(dkq.a)
          b         <- client(dkq.b)
          started   <- Clock.instant
          scheduled <- a.schedule(name, message("k1", "m1"), 1.second)
          claimed   <- b.dequeue(name, 10.seconds, 1)
          arrived   <- Clock.instant
          _         <- settle(b, claimed)
          waited     = Duration.fromInterval(started, arrived)
        yield assertTrue(
          scheduled == Scheduled.Recorded(1.second),
          ids(claimed) == Chunk(MessageId("m1")),
          waited >= 900.millis,
          waited <= 3.seconds,
        )
      },
      test("a schedule repeating an id still scheduled is ignored on the other instance too") {
        for
          dkq    <- ZIO.service[Deployment]
          name    = dkq.queue("schedule-first-wins")
          a      <- client(dkq.a)
          b      <- client(dkq.b)
          first  <- a.schedule(name, message("k1", "m1"), 1.minute)
          second <- b.schedule(name, message("k1", "m1"), 1.second)
        yield assertTrue(
          first == Scheduled.Recorded(1.minute),
          second match
            case Scheduled.Ignored(dueIn) => dueIn > 1.second
            case _                        => false,
        )
      },
      test("a delay beyond the horizon is rejected, with the service's reason") {
        for
          dkq     <- ZIO.service[Deployment]
          a       <- client(dkq.a)
          refused <- a.schedule(dkq.queue("schedule-too-late"), message("k1", "m1"), 8.days).either
        yield assertTrue(
          refused match
            case Left(ServiceError.Rejected(reason)) => reason.contains("deliver_after cannot exceed")
            case _                                   => false
        )
      },
      test("a scheduled message whose id is still queued when it falls due is the same message, delivered once") {
        for
          dkq     <- ZIO.service[Deployment]
          name     = dkq.queue("schedule-discarded")
          a       <- client(dkq.a)
          b       <- client(dkq.b)
          _       <- a.enqueue(name, message("k1", "m1"))
          _       <- b.schedule(name, message("k1", "m1"), 500.millis)
          _       <- ZIO.sleep(1500.millis)
          claimed <- a.dequeue(name, 5.seconds, 10)
          _       <- settle(a, claimed)
          again   <- a.dequeue(name, 1.second, 10)
        yield assertTrue(ids(claimed) == Chunk(MessageId("m1")), again == Dequeued.Idle)
      },
      test("a schedule outlives the instance that took it: the other one moves it on its backstop") {
        // SIGKILL right after the schedule is recorded: the early tick the accepting instance asked for dies
        // with it, so only the surviving instance's backstop (5 s here) can move the message.
        for
          dkq     <- ZIO.service[Deployment]
          name     = dkq.queue("schedule-survives")
          b       <- client(dkq.b)
          a       <- client(dkq.a)
          _       <- a.schedule(name, message("k1", "m1"), 1.second)
          _       <- Compose.kill(dkq.a.name)
          claimed <- b.dequeue(name, 20.seconds, 1)
          _       <- settle(b, claimed)
        yield assertTrue(ids(claimed) == Chunk(MessageId("m1")))
      } @@ TestAspect.after(Compose.revive("dkq-a").orDie) @@ TestAspect.ifEnvNotSet("DKQ_E2E_ENDPOINTS"),
    ).provideShared(Deployment.layer, Scope.default)
      @@ TestAspect.withLiveClock
      @@ TestAspect.sequential
      @@ TestAspect.timeout(5.minutes)

  private object Support {

    /**
     * A queue client pointed at one instance, closed with the suite's scope.
     *
     * @param at which instance to dial
     * @return the client; aborts with `ServiceError` when it cannot be dialled
     */
    def client(at: Instance): ZIO[Scope, ServiceError, QueueClient] =
      QueueClient.scoped(Endpoint(at.host, at.port))

    /**
     * A message for a key, its id also its cargo.
     *
     * @param key the key
     * @param id the id
     * @return the message
     */
    def message(key: String, id: String): Message.Outgoing =
      Message.Outgoing(MessageKey(key), MessageId(id), "test.Text", "text/plain", Chunk.fromArray(id.getBytes("UTF-8")))

    /**
     * The ids a dequeue was handed, in order.
     *
     * @param dequeued what it answered
     * @return the ids; empty when it was idle
     */
    def ids(dequeued: Dequeued): Chunk[MessageId] =
      dequeued match
        case Dequeued.Claimed(claim) => claim.messages.toChunk.map(messageId)
        case Dequeued.Idle           => Chunk.empty

    /**
     * One delivered message's id.
     *
     * @param delivered the message
     * @return its id
     */
    def messageId(delivered: Message.Incoming): MessageId = delivered.id

    /**
     * Acknowledge everything a dequeue was handed, so nothing is left for a later test.
     *
     * @param client the client it was dequeued with
     * @param dequeued what it answered
     * @return noop once settled; aborts with `ServiceError` when the settle does not land
     */
    def settle(client: QueueClient, dequeued: Dequeued): IO[ServiceError, Unit] =
      dequeued match
        case Dequeued.Claimed(claim) => client.settle(claim.receipt, claim.messages.toChunk.map(done), Duration.Zero).unit
        case Dequeued.Idle           => ZIO.unit

    /**
     * A message reported done.
     *
     * @param delivered the message
     * @return its verdict
     */
    def done(delivered: Message.Incoming): Verdict = Verdict(delivered.id, Verdict.Outcome.Done)
  }
