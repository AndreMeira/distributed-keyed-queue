package homelab.keyedqueue


import com.google.protobuf.duration.Duration as ProtoDuration
import homelab.keyedqueue.application.grpc.GrpcApplication
import homelab.keyedqueue.infrastructure.configuration.QueueConfig
import homelab.keyedqueue.v1.*
import homelab.keyedqueue.v1.ZioKeyedQueue.KeyedQueueClient
import io.grpc.ManagedChannelBuilder
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
                         val _ = java.lang.System.setProperty("api.version", "1.40")
                         val started: GenericContainer[?] = GenericContainer("valkey/valkey:8.1-alpine")
                         started.setExposedPorts(java.util.List.of(Integer.valueOf(6379)))
                         started.start()
                         started
                     )(container => ZIO.attemptBlocking(container.stop()).ignore)
        url        = s"redis://${container.getHost}:${container.getMappedPort(6379)}"
        config     = QueueConfig(url, port, 30.seconds, 1.second, 100, 2, 5.seconds)
        _         <- GrpcApplication.serve(config).forkScoped
        _         <- ZIO.sleep(1.second) // let the server bind before the client dials
        client    <- KeyedQueueClient.scoped(
                       ZManagedChannel(ManagedChannelBuilder.forAddress("localhost", port).usePlaintext())
                     )
      yield client

  private def envelope(key: String, body: String): Envelope =
    Envelope(
      key = key,
      messageId = s"$key-$body",
      payloadType = "test.Message/v1",
      encoding = Encoding.ENCODING_JSON,
      payload = com.google.protobuf.ByteString.copyFromUtf8(body),
    )

  def spec: Spec[TestEnvironment & Scope, Any] = suite("KeyedQueue over gRPC")(
    test("enqueue, dequeue, settle — the loop a consumer writes") {
      for
        client   <- ZIO.service[KeyedQueueClient]
        _        <- client.enqueue(EnqueueRequest("jobs", Some(envelope("k1", "hello"))))
        reply    <- client.dequeue(DequeueRequest("jobs", Some(ProtoDuration(seconds = 2))))
        delivery  = reply.delivery
        settled  <- ZIO.foreach(delivery)(d =>
                      client.settle(SettleRequest(d.receipt, Outcome.OUTCOME_DONE))
                    )
        empty    <- client.dequeue(DequeueRequest("jobs", Some(ProtoDuration(seconds = 1))))
      yield assertTrue(
        delivery.flatMap(_.envelope).map(_.payload.toStringUtf8).contains("hello"),
        delivery.map(_.attempt).contains(1),
        delivery.exists(_.receipt.nonEmpty),
        settled.map(_.applied).contains(Applied.APPLIED_OK),
        empty.delivery.isEmpty, // the queue is drained, and a timeout is an empty response, not an error
      )
    },
    test("a settle replayed with the same receipt is reported stale") {
      // What an at-least-once RPC does on a retry. The second must not apply, or the key would be queued
      // twice and two consumers could hold it.
      for
        client   <- ZIO.service[KeyedQueueClient]
        _        <- client.enqueue(EnqueueRequest("replay", Some(envelope("k1", "once"))))
        reply    <- client.dequeue(DequeueRequest("replay", Some(ProtoDuration(seconds = 2))))
        receipt   = reply.delivery.map(_.receipt).getOrElse("")
        first    <- client.settle(SettleRequest(receipt, Outcome.OUTCOME_DONE))
        second   <- client.settle(SettleRequest(receipt, Outcome.OUTCOME_DONE))
      yield assertTrue(first.applied == Applied.APPLIED_OK, second.applied == Applied.APPLIED_STALE)
    },
    test("heartbeat renews what is held and names what is not") {
      for
        client   <- ZIO.service[KeyedQueueClient]
        _        <- client.enqueue(EnqueueRequest("beats", Some(envelope("k1", "work"))))
        reply    <- client.dequeue(DequeueRequest("beats", Some(ProtoDuration(seconds = 2))))
        receipt   = reply.delivery.map(_.receipt).getOrElse("")
        beat     <- client.heartbeat(HeartbeatRequest(Seq(receipt, "not-a-receipt")))
      yield assertTrue(
        beat.stale == Seq("not-a-receipt"), // the real one was renewed; the nonsense one named
        beat.renewedUntil.isDefined,
      )
    },
    test("a message with no encoding is refused, and one with no key too") {
      for
        client  <- ZIO.service[KeyedQueueClient]
        noCodec <- client.enqueue(EnqueueRequest("bad", Some(envelope("k1", "x").copy(encoding = Encoding.ENCODING_UNSPECIFIED)))).exit
        noKey   <- client.enqueue(EnqueueRequest("bad", Some(envelope("", "x")))).exit
      yield assertTrue(noCodec.isFailure, noKey.isFailure)
    },
  ).provideShared(running) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.timeout(3.minutes)
