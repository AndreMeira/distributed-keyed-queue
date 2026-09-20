package homelab.keyedqueue.client.lock


import homelab.keyedqueue.client.{ Endpoint, ServiceError }
import homelab.keyedqueue.client.lock.managed.GrpcClient
import homelab.keyedqueue.client.lock.model.{ Acquired, Hold, Receipt, Refreshed }
import homelab.keyedqueue.v1.ZioKeyedLockService.KeyedLockClient
import io.grpc.ManagedChannelBuilder
import scalapb.zio_grpc.ZManagedChannel
import zio.*


/**
 * The lock service's four calls, in Scala types.
 *
 * One method per RPC and nothing withheld: the fence is in [[model.Hold]], the wait is the caller's, and
 * try-acquire is its own verb rather than a wait of zero, which the service refuses. What the wire states
 * as a flag and three fields arrives here as a choice.
 */
trait LockClient:

  /**
   * Take the named lock, waiting up to `maxWait` for a holder to release it.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives without a refresh
   * @param maxWait how long to wait for it; the service clamps this to its own ceiling
   * @return the grant, or `Unavailable` when the wait elapsed; aborts with `Rejected` when the request is
   *         malformed, or `Unreachable` when the deployment did not answer
   */
  def acquire(name: String, ttl: Duration, maxWait: Duration): IO[ServiceError, Acquired]

  /**
   * Take the named lock only if it is free and unqueued right now.
   *
   * @param name the lock to take
   * @param ttl how long the hold survives without a refresh
   * @return the grant, or `Unavailable` when somebody holds it or is already queued for it; aborts with
   *         `Rejected` when the request is malformed, or `Unreachable` when the deployment did not answer
   */
  def tryAcquire(name: String, ttl: Duration): IO[ServiceError, Acquired]

  /**
   * Give up a lock this caller holds, so a waiter may take it.
   *
   * @param receipt the handle the grant came with
   * @return whether it applied; `false` says the hold had already been revoked; aborts with `Rejected`
   *         when the receipt is not one the service issued
   */
  def release(receipt: Receipt): IO[ServiceError, Boolean]

  /**
   * Push a held lease forward.
   *
   * @param receipt the handle the grant came with
   * @param ttl how much longer to hold it, from now
   * @return the new deadline, or `Lost` when the hold is gone; aborts with `Rejected` when the receipt is
   *         not one the service issued
   */
  def refresh(receipt: Receipt, ttl: Duration): IO[ServiceError, Refreshed]


object LockClient:

  /**
   * Dial a deployment, closed with the scope.
   *
   * @param endpoint where it answers
   * @return the client; aborts with `Failed` when the channel cannot be built
   */
  def scoped(endpoint: Endpoint): ZIO[Scope, ServiceError, LockClient] =
    scoped(ZManagedChannel(channel(endpoint)), endpoint.patience)

  /**
   * Dial over a channel the caller built, for TLS, interceptors or an in-process transport.
   *
   * @param channel the channel to talk over, closed with the scope
   * @param patience how long a call may take beyond what it was asked to wait for
   * @return the client; aborts with `Failed` when the stub cannot be built
   */
  def scoped(channel: ZManagedChannel, patience: Duration = 10.seconds): ZIO[Scope, ServiceError, LockClient] =
    KeyedLockClient.scoped(channel).map(GrpcClient(_, patience)).mapError(dialling)

  /**
   * What a failure to dial amounts to in this client's terms.
   *
   * The transport raises before any call is made, so there is no status to read: what a caller can do
   * about it is look at the cause.
   *
   * @param cause what the transport raised
   * @return the error to report
   */
  private def dialling(cause: Throwable): ServiceError =
    ServiceError.Failed(cause)

  /**
   * The channel an endpoint describes.
   *
   * @param endpoint where the deployment answers
   * @return the builder, ready to dial
   */
  private def channel(endpoint: Endpoint): ManagedChannelBuilder[?] =
    val builder = ManagedChannelBuilder.forAddress(endpoint.host, endpoint.port)
    if endpoint.plaintext then builder.usePlaintext() else builder
