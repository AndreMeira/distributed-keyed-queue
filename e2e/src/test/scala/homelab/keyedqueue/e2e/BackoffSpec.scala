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
 * now the instance that took the nack asks for a tick at the wait's end. See `docs/architecture/scheduling.md`.
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
          first    <- claim(b, name)
          nacked   <- Clock.instant
          _        <- b.settle(first.receipt, first.messages.toChunk.map(failed), 500.millis)
          again    <- claim(a, name)
          returned <- Clock.instant
          _        <- a.settle(again.receipt, again.messages.toChunk.map(done), Duration.Zero)
          waited    = Duration.fromInterval(nacked, returned)
        yield assertTrue(
          again.messages.map(_.attempt).toChunk == Chunk(2),
          waited >= 450.millis,
          waited <= 1500.millis,
        )
      }
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
     * @return the claim; fails when the queue stayed idle
     */
    def claim(client: QueueClient, queue: String): IO[ServiceError | String, Claim] =
      client.dequeue(queue, 10.seconds, 1).flatMap(claimed(queue))

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
