package homelab.keyedqueue.client.lock.managed


import com.google.protobuf.duration.Duration as ProtoDuration
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.codec.{ LockCodecs, Protos }
import homelab.keyedqueue.client.lock.LockClient
import homelab.keyedqueue.client.lock.model.{ Acquired, Receipt, Refreshed }
import homelab.keyedqueue.v1
import homelab.keyedqueue.v1.ZioKeyedLockService.KeyedLockClient
import io.grpc.{ CallOptions, StatusException }
import scalapb.zio_grpc.ClientTransform
import zio.*

import java.util.concurrent.TimeUnit


/**
 * The lock client over the generated stub.
 *
 * Every method is the same three steps — build the request, make the call, read the answer — so the
 * translation stays in one place and a reader comparing a method to its RPC sees nothing else.
 *
 * Every call carries a deadline, so one that does not come back does not hold the caller: the transport
 * gives up and the failure is read like any other. An acquire is allowed its own wait on top, because
 * waiting for a holder to release is what it is for.
 *
 * @param stub the generated client, already dialled
 * @param patience how long a call may take beyond what it was asked to wait for
 */
final private[client] class GrpcClient(stub: KeyedLockClient, patience: Duration) extends LockClient:

  /**
   * One `Acquire` call.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives without a refresh
   * @param maxWait how long to wait for a holder to release
   * @return what the service answered, read as a choice; aborts with the [[ServiceError]] a transport
   *         failure amounts to
   */
  override def acquire(name: String, ttl: Duration, maxWait: Duration): IO[ServiceError, Acquired] =
    call(within(maxWait + patience).acquire(v1.AcquireRequest(name, proto(ttl), proto(maxWait)))).map(LockCodecs.decode).absolve

  /**
   * One `TryAcquire` call, which carries no wait for the service to clamp.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives without a refresh
   * @return what the service answered, read as a choice; aborts with the [[ServiceError]] a transport
   *         failure amounts to
   */
  override def tryAcquire(name: String, ttl: Duration): IO[ServiceError, Acquired] =
    call(within(patience).tryAcquire(v1.TryAcquireRequest(name, proto(ttl)))).map(LockCodecs.decode).absolve

  /**
   * One `Release` call, whose answer needs no reading — the wire says a boolean and so does the port.
   *
   * @param receipt the handle the grant came with
   * @return whether it applied; aborts with the [[ServiceError]] a transport failure amounts to
   */
  override def release(receipt: Receipt): IO[ServiceError, Boolean] =
    call(within(patience).release(v1.ReleaseRequest(receipt))).map(_.released)

  /**
   * One `Refresh` call.
   *
   * @param receipt the handle the grant came with
   * @param ttl how much longer to hold it, from now
   * @return what the service answered, read as a choice; aborts with the [[ServiceError]] a transport
   *         failure amounts to
   */
  override def refresh(receipt: Receipt, ttl: Duration): IO[ServiceError, Refreshed] =
    call(within(patience).refresh(v1.RefreshRequest(receipt, proto(ttl)))).map(LockCodecs.decode).absolve

  /**
   * The stub, for a call the transport gives up on after a while.
   *
   * A deadline rather than an interruption: releasing runs where a caller's interruption does not reach,
   * so a call that never answers there is one nothing else can stop.
   *
   * @param deadline how long the call may take
   * @return the stub, bounded
   */
  private def within(deadline: Duration): KeyedLockClient =
    stub.transform(ClientTransform.mapCallOptions(by(deadline)))

  /**
   * A deadline on a call's options.
   *
   * @param deadline how long the call may take
   * @param options what the call would have been made with
   * @return them, with the deadline
   */
  private def by(deadline: Duration)(options: CallOptions): CallOptions =
    options.withDeadlineAfter(deadline.toMillis, TimeUnit.MILLISECONDS)

  /**
   * Make one call, reporting a transport failure in this client's terms.
   *
   * @param rpc the call to make
   * @tparam A what it answers with
   * @return the answer; aborts with the [[ServiceError]] the failure amounts to
   */
  private def call[A](rpc: IO[StatusException, A]): IO[ServiceError, A] =
    rpc.mapError(Protos.failure)

  /**
   * A duration as the proto carries it, wrapped as the generated request expects.
   *
   * @param duration how long
   * @return it, present
   */
  private def proto(duration: Duration): Option[ProtoDuration] =
    Some(Protos.encode(duration))
