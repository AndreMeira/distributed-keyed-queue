package homelab.keyedqueue.client.lock


import com.google.protobuf.duration.Duration as ProtoDuration
import com.google.protobuf.timestamp.Timestamp
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.lock.model.{ Acquired, Fence, Hold, Receipt, Refreshed }
import homelab.keyedqueue.v1
import homelab.keyedqueue.v1.ZioKeyedLockService.KeyedLock
import io.grpc.inprocess.{ InProcessChannelBuilder, InProcessServerBuilder }
import io.grpc.{ Status, StatusException }
import scalapb.zio_grpc.{ ScopedServer, ServiceList, ZManagedChannel }
import zio.*
import zio.test.*

import java.time.Instant


/**
 * The client over a real channel, against a service that records what arrived.
 *
 * What only this suite can show: that the four calls put the right thing on the wire and read the answer
 * back. The codec specs above it work on messages that were handed to them, so a request built wrong, or a
 * status read wrong, would pass everything else in this module.
 */
object GrpcLockClientSpec extends ZIOSpecDefault:

  private val until   = Timestamp(1_700_000_000L, 0)
  private val moment  = Instant.ofEpochSecond(1_700_000_000L)
  private val span    = ProtoDuration(30L, 0)
  private val granted = v1.AcquireResponse(acquired = true, "receipt", 7L, Some(until), Some(span))
  private val renewed = v1.RefreshResponse(renewed = true, Some(until), Some(span))

  /** A service that answers the same way every time, and keeps what it was sent. */
  final private class Recording(seen: Ref[Chunk[Any]]) extends KeyedLock:
    override def acquire(request: v1.AcquireRequest): IO[StatusException, v1.AcquireResponse]       =
      seen.update(_ :+ request).as(granted)
    override def tryAcquire(request: v1.TryAcquireRequest): IO[StatusException, v1.AcquireResponse] =
      seen.update(_ :+ request).as(granted)
    override def release(request: v1.ReleaseRequest): IO[StatusException, v1.ReleaseResponse]       =
      seen.update(_ :+ request).as(v1.ReleaseResponse(released = true))
    override def refresh(request: v1.RefreshRequest): IO[StatusException, v1.RefreshResponse]       =
      seen.update(_ :+ request).as(renewed)

  /** A service that refuses everything with one status, for the reading of a failure. */
  final private class Refusing(status: Status) extends KeyedLock:
    private def refused: IO[StatusException, Nothing]                                               =
      ZIO.fail(StatusException(status))
    override def acquire(request: v1.AcquireRequest): IO[StatusException, v1.AcquireResponse]       = refused
    override def tryAcquire(request: v1.TryAcquireRequest): IO[StatusException, v1.AcquireResponse] = refused
    override def release(request: v1.ReleaseRequest): IO[StatusException, v1.ReleaseResponse]       = refused
    override def refresh(request: v1.RefreshRequest): IO[StatusException, v1.RefreshResponse]       = refused

  /**
   * Serve one lock service and dial it, both closed with the scope.
   *
   * @param service what answers the calls
   * @return a client talking to it over an in-process channel
   */
  private def served(service: KeyedLock): ZIO[Scope, Throwable | ServiceError, LockClient] =
    val name = InProcessServerBuilder.generateName()
    for
      _      <- ScopedServer.fromServiceList(
                  InProcessServerBuilder.forName(name).directExecutor(),
                  ServiceList.add(service),
                )
      client <- LockClient.scoped(ZManagedChannel(InProcessChannelBuilder.forName(name).directExecutor()))
    yield client

  /**
   * A client over a service that refuses with one status.
   *
   * @param status what every call comes back with
   * @return what the client made of a refused acquire
   */
  private def refusedBy(status: Status): ZIO[Scope, Throwable | ServiceError, Either[ServiceError, Acquired]] =
    for
      client <- served(Refusing(status))
      answer <- client.acquire("a-key", 1.second, 1.second).either
    yield answer

  def spec: Spec[TestEnvironment & Scope, Any] = suite("GrpcClient")(
    test("an acquire carries the name and both spans, and a grant reads back as a hold") {
      for
        seen   <- Ref.make(Chunk.empty[Any])
        client <- served(Recording(seen))
        answer <- client.acquire("the-key", ttl = 30.seconds, maxWait = 5.seconds)
        sent   <- seen.get
      yield assertTrue(
        sent == Chunk(v1.AcquireRequest("the-key", Some(ProtoDuration(30L, 0)), Some(ProtoDuration(5L, 0)))),
        answer == Acquired.Granted(Hold(Receipt("receipt"), Fence(7L), moment, 30.seconds)),
      )
    },
    test("a try-acquire carries the name and the ttl, and no wait at all") {
      for
        seen   <- Ref.make(Chunk.empty[Any])
        client <- served(Recording(seen))
        answer <- client.tryAcquire("the-key", ttl = 30.seconds)
        sent   <- seen.get
      yield assertTrue(
        sent == Chunk(v1.TryAcquireRequest("the-key", Some(ProtoDuration(30L, 0)))),
        answer == Acquired.Granted(Hold(Receipt("receipt"), Fence(7L), moment, 30.seconds)),
      )
    },
    test("a release names the receipt and answers whether it applied") {
      for
        seen    <- Ref.make(Chunk.empty[Any])
        client  <- served(Recording(seen))
        applied <- client.release(Receipt("a-receipt"))
        sent    <- seen.get
      yield assertTrue(sent == Chunk(v1.ReleaseRequest("a-receipt")), applied)
    },
    test("a refresh names the receipt and the span, and the renewal reads back with its lease") {
      for
        seen   <- Ref.make(Chunk.empty[Any])
        client <- served(Recording(seen))
        answer <- client.refresh(Receipt("a-receipt"), ttl = 30.seconds)
        sent   <- seen.get
      yield assertTrue(
        sent == Chunk(v1.RefreshRequest("a-receipt", Some(ProtoDuration(30L, 0)))),
        answer == Refreshed.Renewed(moment, 30.seconds),
      )
    },
    test("a malformed request comes back as the caller's to fix, carrying what the service said") {
      refusedBy(Status.INVALID_ARGUMENT.withDescription("a lock name is required"))
        .map(answer => assertTrue(answer == Left(ServiceError.Rejected("a lock name is required"))))
    },
    test("an unreachable deployment comes back as worth retrying") {
      refusedBy(Status.UNAVAILABLE)
        .map(answer => assertTrue(answer.left.exists(_.isInstanceOf[ServiceError.Unreachable])))
    },
    test("anything else comes back reported rather than interpreted") {
      refusedBy(Status.INTERNAL)
        .map(answer => assertTrue(answer.left.exists(_.isInstanceOf[ServiceError.Failed])))
    },
  )
