package homelab.keyedqueue


import com.google.protobuf.duration.Duration as ProtoDuration
import homelab.keyedqueue.application.grpc.v1.GrpcApplication
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.v1.*
import homelab.keyedqueue.v1.ZioKeyedQueueService.KeyedQueueClient
import io.grpc.{ ManagedChannelBuilder, Status, StatusException }
import org.testcontainers.containers.GenericContainer
import scalapb.zio_grpc.ZManagedChannel
import zio.*
import zio.test.*


/**
 * The API a consumer actually sees: four calls, over a real server, against a real substrate.
 *
 * The store spec proves the guarantees; this proves the wiring — that a client can enqueue, block for work,
 * settle it, and be told what it still holds, with no knowledge of keys, leases or Redis.
 */
object GrpcSpec extends ZIOSpecDefault:

  private val port = 19_099

  /** A Valkey container, the service on a port, and a client pointed at it. */
  private val running: ZLayer[Any, Any, KeyedQueueClient] =
    ZLayer.scoped:
      for
        container <- ZIO.acquireRelease(
                       ZIO.attemptBlocking:
                         val _                            = java.lang.System.setProperty("api.version", "1.40")
                         val started: GenericContainer[?] = GenericContainer("valkey/valkey:8.1-alpine")
                         started.setExposedPorts(java.util.List.of(Integer.valueOf(6379)))
                         started.start()
                         started
                     )(container => ZIO.attemptBlocking(container.stop()).ignore)
        url        = s"redis://${container.getHost}:${container.getMappedPort(6379)}"
        config     = QueueConfig(url, cluster = false, port, 30.seconds, 1.second, 100, 2, 5.seconds, maxBatchLimit = 32)
        _         <- GrpcApplication.serve.provide(ZLayer.succeed(config)).forkScoped
        _         <- ZIO.sleep(1.second) // let the server bind before the client dials
        client    <- KeyedQueueClient.scoped(
                       ZManagedChannel(ManagedChannelBuilder.forAddress("localhost", port).usePlaintext())
                     )
      yield client

  private def message(key: String, body: String): Message =
    Message(
      key = key,
      messageId = s"$key-$body",
      payloadType = "test.Message/v1",
      encoding = Encoding.ENCODING_JSON,
      payload = com.google.protobuf.ByteString.copyFromUtf8(body),
    )

  /** One message's outcome, by id. */
  private def done(id: String): MessageOutcome = MessageOutcome(id, Outcome.OUTCOME_DONE)

  /** Settle every message a claim handed over, with the same verdict. */
  private def settle(reply: DequeueResponse, outcome: Outcome): SettleRequest =
    SettleRequest(reply.receipt, outcomes = claimed(reply).map(d => MessageOutcome(d.messageId, outcome)))

  /** Everything a claim handed over, head first — the shape a consumer actually iterates. */
  private def claimed(reply: DequeueResponse): Seq[Delivery] = reply.head.toSeq ++ reply.tail

  /** What a claim is carrying, as text, in the order it was handed over. */
  private def bodies(reply: DequeueResponse): Seq[String] =
    claimed(reply).flatMap(_.message).map(_.payload.toStringUtf8)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("KeyedQueue over gRPC")(
    test("enqueue, dequeue, settle — the loop a consumer writes") {
      for
        client  <- ZIO.service[KeyedQueueClient]
        _       <- client.enqueue(EnqueueRequest("jobs", Some(message("k1", "hello"))))
        reply   <- client.dequeue(DequeueRequest("jobs", Some(ProtoDuration(seconds = 2))))
        settled <- client.settle(settle(reply, Outcome.OUTCOME_DONE))
        empty   <- client.dequeue(DequeueRequest("jobs", Some(ProtoDuration(seconds = 1))))
      yield assertTrue(
        bodies(reply) == Seq("hello"),
        reply.head.map(_.attempt) == Some(1),
        reply.receipt.nonEmpty,
        reply.leaseExpiresAt.isDefined,
        settled.applied == Applied.APPLIED_OK,
        empty.head.isEmpty, // the queue is drained, and a timeout is an empty response, not an error
      )
    },
    test("a claim hands over a batch, and each message is settled by name") {
      for
        client  <- ZIO.service[KeyedQueueClient]
        _       <- ZIO.foreachDiscard(1 to 4)(n => client.enqueue(EnqueueRequest("batch", Some(message("k1", s"v$n")))))
        reply   <- client.dequeue(DequeueRequest("batch", Some(ProtoDuration(seconds = 2)), maxBatch = 3))
        settled <- client.settle(settle(reply, Outcome.OUTCOME_DONE))
        next    <- client.dequeue(DequeueRequest("batch", Some(ProtoDuration(seconds = 2))))
      yield assertTrue(
        bodies(reply) == Seq("v1", "v2", "v3"),
        reply.backlogDepth == 3 - 3 + 1, // v4, still queued behind the batch
        settled.applied == Applied.APPLIED_OK,
        bodies(next) == Seq("v4"),
      )
    },
    test("a consumer can settle part of its batch and keep the rest") {
      // Conflation, and the shape that makes it possible: the consumer sees the whole batch, decides which
      // are superseded, and says so message by message. The key stays its own until nothing is owed.
      for
        client  <- ZIO.service[KeyedQueueClient]
        _       <- ZIO.foreachDiscard(1 to 3)(n => client.enqueue(EnqueueRequest("partial", Some(message("k1", s"v$n")))))
        reply   <- client.dequeue(DequeueRequest("partial", Some(ProtoDuration(seconds = 2)), maxBatch = 3))
        ids      = claimed(reply).map(_.messageId)
        // v1 and v2 are superseded by v3, so they are acknowledged without being worked.
        partial <- client.settle(SettleRequest(reply.receipt, outcomes = ids.take(2).map(done).toSeq))
        // Still owed v3, so nobody else may have the key.
        blocked <- client.dequeue(DequeueRequest("partial", Some(ProtoDuration(seconds = 1))))
        _       <- client.settle(SettleRequest(reply.receipt, outcomes = ids.drop(2).map(done).toSeq))
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
        _      <- client.enqueue(EnqueueRequest("replay", Some(message("k1", "once"))))
        reply  <- client.dequeue(DequeueRequest("replay", Some(ProtoDuration(seconds = 2))))
        first  <- client.settle(settle(reply, Outcome.OUTCOME_DONE))
        second <- client.settle(settle(reply, Outcome.OUTCOME_DONE))
      yield assertTrue(first.applied == Applied.APPLIED_OK, second.applied == Applied.APPLIED_STALE)
    },
    test("heartbeat renews what is held and names what is not") {
      for
        client <- ZIO.service[KeyedQueueClient]
        _      <- client.enqueue(EnqueueRequest("beats", Some(message("k1", "work"))))
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
        noCodec <- client.enqueue(EnqueueRequest("bad", Some(message("k1", "x").copy(encoding = Encoding.ENCODING_UNSPECIFIED)))).exit
        noKey   <- client.enqueue(EnqueueRequest("bad", Some(message("", "x")))).exit
      yield assertTrue(noCodec.isFailure, noKey.isFailure)
    },
    test("a request with two problems is refused once, naming both") {
      // Accumulating validation is only worth having if it survives to the caller: one INVALID_ARGUMENT
      // carrying every reason, rather than one round trip per mistake.
      for
        client <- ZIO.service[KeyedQueueClient]
        failed <- client.enqueue(EnqueueRequest("", Some(message("", "x")))).flip
        status  = failed match
                    case error: StatusException => Option(error.getStatus)
                    case _                      => None
      yield assertTrue(
        status.map(_.getCode).contains(Status.Code.INVALID_ARGUMENT),
        status.flatMap(reported => Option(reported.getDescription)).exists(_.contains("a queue name is required")),
        status.flatMap(reported => Option(reported.getDescription)).exists(_.contains("a message key is required")),
      )
    },
  ).provideShared(running) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.timeout(3.minutes)
