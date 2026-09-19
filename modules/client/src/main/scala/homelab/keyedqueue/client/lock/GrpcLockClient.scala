package homelab.keyedqueue.client.lock


import homelab.keyedqueue.client.LockError
import homelab.keyedqueue.client.codec.LockCodecs
import homelab.keyedqueue.v1
import homelab.keyedqueue.v1.ZioKeyedLockService.KeyedLockClient
import io.grpc.StatusException
import zio.*


/**
 * The lock client over the generated stub.
 *
 * Every method is the same three steps — build the request, make the call, read the answer — so the
 * translation stays in one place and a reader comparing a method to its RPC sees nothing else.
 *
 * @param stub the generated client, already dialled
 */
final private[client] class GrpcLockClient(stub: KeyedLockClient) extends LockClient:

  /**
   * One `Acquire` call.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives without a refresh
   * @param maxWait how long to wait for a holder to release
   * @return what the service answered, read as a choice; aborts with the [[LockError]] a transport
   *         failure amounts to
   */
  override def acquire(name: String, ttl: Duration, maxWait: Duration): IO[LockError, Acquired] =
    call(stub.acquire(v1.AcquireRequest(name, wire(ttl), wire(maxWait)))).flatMap(LockCodecs.acquired)

  /**
   * One `TryAcquire` call, which carries no wait for the service to clamp.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives without a refresh
   * @return what the service answered, read as a choice; aborts with the [[LockError]] a transport
   *         failure amounts to
   */
  override def tryAcquire(name: String, ttl: Duration): IO[LockError, Acquired] =
    call(stub.tryAcquire(v1.TryAcquireRequest(name, wire(ttl)))).flatMap(LockCodecs.acquired)

  /**
   * One `Release` call, whose answer needs no reading — the wire says a boolean and so does the port.
   *
   * @param receipt the handle the grant came with
   * @return whether it applied; aborts with the [[LockError]] a transport failure amounts to
   */
  override def release(receipt: Receipt): IO[LockError, Boolean] =
    call(stub.release(v1.ReleaseRequest(receipt))).map(_.released)

  /**
   * One `Refresh` call.
   *
   * @param receipt the handle the grant came with
   * @param ttl how much longer to hold it, from now
   * @return what the service answered, read as a choice; aborts with the [[LockError]] a transport
   *         failure amounts to
   */
  override def refresh(receipt: Receipt, ttl: Duration): IO[LockError, Refreshed] =
    call(stub.refresh(v1.RefreshRequest(receipt, wire(ttl)))).flatMap(LockCodecs.refreshed)

  /**
   * Make one call, reporting a transport failure in this client's terms.
   *
   * @param rpc the call to make
   * @tparam A what it answers with
   * @return the answer; aborts with the [[LockError]] the failure amounts to
   */
  private def call[A](rpc: IO[StatusException, A]): IO[LockError, A] =
    rpc.mapError(LockCodecs.failure)

  /**
   * A duration as the wire carries it, wrapped as the generated request expects.
   *
   * @param duration how long
   * @return it, present
   */
  private def wire(duration: Duration): Option[com.google.protobuf.duration.Duration] =
    Some(LockCodecs.toWire(duration))
