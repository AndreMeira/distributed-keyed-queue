package homelab.keyedqueue.client.lock


import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.v1.ZioKeyedLockService.KeyedLockClient
import io.grpc.ManagedChannelBuilder
import scalapb.zio_grpc.ZManagedChannel
import zio.*


/**
 * The lock service's four calls, in Scala types.
 *
 * One method per RPC and nothing withheld: the fence is in [[Hold]], the wait is the caller's, and
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
   * Where a deployment answers, and how to reach it.
   *
   * @param host where it listens
   * @param port the port it serves on
   * @param plaintext whether to dial without TLS, which is the homelab's arrangement inside a cluster
   */
  final case class Config(host: String, port: Int, plaintext: Boolean = true)

  /**
   * Dial a deployment, closed with the scope.
   *
   * @param config where it answers
   * @return the client; aborts when the channel cannot be built
   */
  def scoped(config: Config): ZIO[Scope, Throwable, LockClient] =
    scoped(ZManagedChannel(channel(config)))

  /**
   * Dial over a channel the caller built, for TLS, interceptors or an in-process transport.
   *
   * @param channel the channel to talk over, closed with the scope
   * @return the client; aborts when the stub cannot be built
   */
  def scoped(channel: ZManagedChannel): ZIO[Scope, Throwable, LockClient] =
    KeyedLockClient.scoped(channel).map(GrpcLockClient(_))

  /**
   * The channel a [[Config]] describes.
   *
   * @param config where the deployment answers
   * @return the builder, ready to dial
   */
  private def channel(config: Config): ManagedChannelBuilder[?] =
    val builder = ManagedChannelBuilder.forAddress(config.host, config.port)
    if config.plaintext then builder.usePlaintext() else builder
