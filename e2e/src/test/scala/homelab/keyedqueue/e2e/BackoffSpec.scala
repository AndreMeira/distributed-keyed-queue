package homelab.keyedqueue.e2e


import homelab.keyedqueue.client.queue.QueueClient
import homelab.keyedqueue.client.queue.model.*
import homelab.keyedqueue.client.{ Endpoint, ServiceError }
import zio.*
import zio.test.*


/**
 * A nack's backoff, as a consumer of the client sees it, against the deployment: the key comes back when its wait
 * ends, through either instance, and not before.
 *
 * Before the backoff mover, the watchdog's sweep released it, up to a `sweep-interval` late (2 s on the e2e stacks);
 * now the instance that took the nack asks for a tick at the wait's end, and every instance's backoff mover covers every
 * partition, so a survivor releases a key whatever queues it has served. See `docs/architecture/scheduling.md`.
 */
object BackoffSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("backoffs, against the deployment")(
      test("a key nacked with a backoff on one instance comes back through the other when the wait ends") {
        for
          dkq      <- ZIO.service[Deployment]
          name      = dkq.queue("backoff-ends")
          a        <- client(dkq.a)
          b        <- client(dkq.b)
          _        <- a.enqueue(name, message("k1", "m1"))
          first    <- claim(b, name, 10.seconds)
          nacked   <- Clock.instant
          _        <- b.settle(first.receipt, first.messages.toChunk.map(failed), 500.millis)
          again    <- claim(a, name, 10.seconds)
          returned <- Clock.instant
          _        <- a.settle(again.receipt, again.messages.toChunk.map(done), Duration.Zero)
          waited    = Duration.fromInterval(nacked, returned)
        yield assertTrue(
          again.messages.map(_.attempt).toChunk == Chunk(2),
          waited >= 450.millis,
          waited <= 1500.millis,
        )
      },
      test("a backoff outlives the instance that took the nack: the other releases it on its backstop, unasked") {
        // SIGKILL right after the nack: the tick the nacking instance asked for dies with it. The survivor has never
        // served this queue, so it does not touch it until well past its own backstop (5 s) plus the wait, then
        // takes the key with a short wait. Under the watchdog's sweep, which only covered queues an instance had
        // served, the key would still be held back then, and the short wait would come back empty.
        for
          dkq   <- ZIO.service[Deployment]
          name   = dkq.queue("backoff-survives")
          a     <- client(dkq.a)
          b     <- client(dkq.b)
          _     <- a.enqueue(name, message("k1", "m1"))
          first <- claim(a, name, 10.seconds)
          _     <- a.settle(first.receipt, first.messages.toChunk.map(failed), 1.second)
          _     <- Compose.kill(dkq.a.name)
          _     <- ZIO.sleep(8.seconds)
          again <- claim(b, name, 1.second)
          _     <- b.settle(again.receipt, again.messages.toChunk.map(done), Duration.Zero)
        yield assertTrue(again.messages.map(_.attempt).toChunk == Chunk(2))
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
     * Dequeue one claim, which these tests always expect within the wait.
     *
     * @param client the client to dequeue with
     * @param queue the queue
     * @param wait how long to wait for it
     * @return the claim; fails when the queue stayed idle
     */
    def claim(client: QueueClient, queue: String, wait: Duration): IO[ServiceError | String, Claim] =
      client.dequeue(queue, wait, 1).flatMap(claimed(queue))

    /**
     * The claim a dequeue answered, or a failure when it answered idle.
     *
     * @param queue the queue, for the failure
     * @param dequeued what the dequeue answered
     * @return the claim; fails when there was none
     */
    def claimed(queue: String)(dequeued: Dequeued): IO[String, Claim] =
      dequeued match
        case Dequeued.Claimed(claim) => ZIO.succeed(claim)
        case Dequeued.Idle           => ZIO.fail(s"nothing came back from $queue")

    /**
     * A message reported failed.
     *
     * @param delivered the message
     * @return its verdict
     */
    def failed(delivered: Message.Incoming): Verdict = Verdict(delivered.id, Verdict.Outcome.Failed)

    /**
     * A message reported done.
     *
     * @param delivered the message
     * @return its verdict
     */
    def done(delivered: Message.Incoming): Verdict = Verdict(delivered.id, Verdict.Outcome.Done)
  }
