package homelab.keyedqueue.client.queue.managed


import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration as ProtoDuration
import com.google.protobuf.timestamp.Timestamp
import homelab.keyedqueue.client.queue.QueueClient
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.model.{ Dequeued, Enqueued, Message, MessageId, MessageKey, Receipt, Settled, Verdict }
import homelab.keyedqueue.v1
import homelab.keyedqueue.v1.ZioKeyedQueueService.KeyedQueue
import io.grpc.inprocess.{ InProcessChannelBuilder, InProcessServerBuilder }
import io.grpc.{ Status, StatusException }
import scalapb.zio_grpc.{ ScopedServer, ServiceList, ZManagedChannel }
import zio.*
import zio.test.*


/**
 * The client over a real channel, against a service that records what arrived.
 *
 * What only this suite can show: that the four calls put the right thing on the wire and read the answer
 * back. The codec spec beside it works on messages handed straight to it, so a request built wrong, or a
 * status read wrong, would pass everything else in this module.
 */
object GrpcClientSpec extends ZIOSpecDefault:

  private val stamp = Timestamp(1_700_000_000L, 0)
  private val span  = ProtoDuration(30L, 0)

  private val delivery = v1.Delivery(
    "m1",
    Some(v1.Message("k1", "m1", "order.v2", "application/json", Some(stamp), ByteString.copyFromUtf8("body"))),
    1,
  )

  private val claim   = v1.DequeueResponse("receipt", Some(delivery), Seq.empty, Some(stamp), 2, Some(span))
  private val renewal = v1.HeartbeatResponse(Seq("gone"), Some(stamp), Some(span))

  /** A service that answers the same way every time, and keeps what it was sent. */
  final private class Recording(seen: Ref[Chunk[Any]]) extends KeyedQueue:
    override def enqueue(request: v1.EnqueueRequest): IO[StatusException, v1.EnqueueResponse]       =
      seen.update(_ :+ request).as(v1.EnqueueResponse(7L))
    override def dequeue(request: v1.DequeueRequest): IO[StatusException, v1.DequeueResponse]       =
      seen.update(_ :+ request).as(claim)
    override def settle(request: v1.SettleRequest): IO[StatusException, v1.SettleResponse]          =
      seen.update(_ :+ request).as(v1.SettleResponse(v1.Applied.APPLIED_OK))
    override def heartbeat(request: v1.HeartbeatRequest): IO[StatusException, v1.HeartbeatResponse] =
      seen.update(_ :+ request).as(renewal)

  /** A service that never answers, for the deadline that stops a caller waiting on it. */
  final private class Hanging extends KeyedQueue:
    override def enqueue(request: v1.EnqueueRequest): IO[StatusException, v1.EnqueueResponse]       = ZIO.never
    override def dequeue(request: v1.DequeueRequest): IO[StatusException, v1.DequeueResponse]       = ZIO.never
    override def settle(request: v1.SettleRequest): IO[StatusException, v1.SettleResponse]          = ZIO.never
    override def heartbeat(request: v1.HeartbeatRequest): IO[StatusException, v1.HeartbeatResponse] = ZIO.never

  /** A service that refuses everything with one status, for the reading of a failure. */
  final private class Refusing(status: Status) extends KeyedQueue:
    private def refused: IO[StatusException, Nothing]                                               =
      ZIO.fail(StatusException(status))
    override def enqueue(request: v1.EnqueueRequest): IO[StatusException, v1.EnqueueResponse]       = refused
    override def dequeue(request: v1.DequeueRequest): IO[StatusException, v1.DequeueResponse]       = refused
    override def settle(request: v1.SettleRequest): IO[StatusException, v1.SettleResponse]          = refused
    override def heartbeat(request: v1.HeartbeatRequest): IO[StatusException, v1.HeartbeatResponse] = refused

  /**
   * Serve one queue service and dial it, both closed with the scope.
   *
   * @param service what answers the calls
   * @return a client talking to it over an in-process channel
   */
  private def served(
    service: KeyedQueue,
    patience: Duration = 10.seconds,
  ): ZIO[Scope, Throwable | ServiceError, QueueClient] =
    val name = InProcessServerBuilder.generateName()
    for
      _      <- ScopedServer.fromServiceList(
                  InProcessServerBuilder.forName(name).directExecutor(),
                  ServiceList.add(service),
                )
      client <- QueueClient.scoped(
                  ZManagedChannel(InProcessChannelBuilder.forName(name).directExecutor()),
                  patience,
                )
    yield client

  def spec: Spec[TestEnvironment & Scope, Any] = suite("GrpcClient")(
    test("an enqueue carries the envelope, the bytes, and when it was sent") {
      for
        seen   <- Ref.make(Chunk.empty[Any])
        client <- served(Recording(seen))
        message = Message.Outgoing(MessageKey("k1"), MessageId("m1"), "order.v2", "application/json", Chunk(9.toByte))
        answer <- client.enqueue("orders", message)
        sent   <- seen.get
        request = sent.collectFirst { case request: v1.EnqueueRequest => request }
      yield assertTrue(
        answer == Enqueued(7L),
        request.map(_.queue).contains("orders"),
        request.flatMap(_.message).map(_.messageId).contains("m1"),
        request.flatMap(_.message).map(_.encoding).contains("application/json"),
        request.flatMap(_.message).map(_.payload.toByteArray.toList).contains(List(9.toByte)),
        request.flatMap(_.message).flatMap(_.sentAt).isDefined,
      )
    },
    test("a dequeue carries the wait and the batch, and the claim reads back") {
      for
        seen   <- Ref.make(Chunk.empty[Any])
        client <- served(Recording(seen))
        answer <- client.dequeue("orders", maxWait = 5.seconds, maxBatch = 4)
        sent   <- seen.get
        request = sent.collectFirst { case request: v1.DequeueRequest => request }
      yield assertTrue(
        request.map(_.queue).contains("orders"),
        request.flatMap(_.maxWait).map(_.seconds).contains(5L),
        request.map(_.maxBatch).contains(4),
        answer != Dequeued.Idle,
      )
    },
    test("a settle carries the receipt, the delay and every outcome") {
      for
        seen    <- Ref.make(Chunk.empty[Any])
        client  <- served(Recording(seen))
        verdicts = Chunk(
                     Verdict(MessageId("m1"), Verdict.Outcome.Done),
                     Verdict(MessageId("m2"), Verdict.Outcome.Failed),
                   )
        answer  <- client.settle(Receipt("receipt"), verdicts, retryAfter = 2.seconds)
        sent    <- seen.get
        request  = sent.collectFirst { case request: v1.SettleRequest => request }
      yield assertTrue(
        answer == Settled.Applied,
        request.map(_.receipt).contains("receipt"),
        request.flatMap(_.retryAfter).map(_.seconds).contains(2L),
        request.map(_.outcomes.map(_.messageId).toList).contains(List("m1", "m2")),
        request
          .map(_.outcomes.map(_.outcome).toList)
          .contains(List(v1.Outcome.OUTCOME_DONE, v1.Outcome.OUTCOME_FAILED)),
      )
    },
    test("a heartbeat names everything held at once, and answers with what is not") {
      for
        seen   <- Ref.make(Chunk.empty[Any])
        client <- served(Recording(seen))
        answer <- client.heartbeat(Chunk(Receipt("one"), Receipt("two")))
        sent   <- seen.get
        request = sent.collectFirst { case request: v1.HeartbeatRequest => request }
      yield assertTrue(
        request.map(_.receipts.toList).contains(List("one", "two")),
        answer.stale == Chunk(Receipt("gone")),
        answer.leaseTtl == 30.seconds,
      )
    },
    test("a call the service never answers comes back once its deadline passes") {
      // Settling runs where a caller's interruption does not reach, so a call with no deadline there is
      // one nothing can stop. The transport gives up instead, and the failure reads like any other.
      for
        hung    <- served(Hanging(), patience = 300.millis)
        outcome <- hung.settle(Receipt("r1"), Chunk.empty, Duration.Zero).either
      yield assertTrue(outcome.isLeft)
    } @@ TestAspect.withLiveClock @@ TestAspect.timeout(20.seconds),
    test("a refusal arrives as the error it amounts to, not as a status") {
      for
        refused  <- served(Refusing(Status.INVALID_ARGUMENT.withDescription("a queue is required")))
        rejected <- refused.dequeue("", 1.second, 1).either
        down     <- served(Refusing(Status.UNAVAILABLE))
        gone     <- down.dequeue("orders", 1.second, 1).either
      yield assertTrue(
        rejected == Left(ServiceError.Rejected("a queue is required")),
        gone.left.exists(_.isInstanceOf[ServiceError.Unreachable]),
      )
    },
  )
