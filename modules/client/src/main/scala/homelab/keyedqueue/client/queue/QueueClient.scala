package homelab.keyedqueue.client.queue


import homelab.keyedqueue.client.queue.managed.GrpcClient
import homelab.keyedqueue.client.queue.model.{ Dequeued, Enqueued, Message, MessageDecoder, MessageEncoder, Receipt, Renewed, Settled, Verdict }
import homelab.keyedqueue.client.{ Endpoint, ServiceError }
import homelab.keyedqueue.v1.ZioKeyedQueueService.KeyedQueueClient
import io.grpc.ManagedChannelBuilder
import scalapb.zio_grpc.ZManagedChannel
import zio.*


/**
 * The queue's four calls, in Scala types.
 *
 * One method per RPC and nothing withheld: outcomes are reported per message rather than per claim, the
 * retry delay is the caller's, and the batch size is stated rather than inferred. Payloads are bytes here
 * — [[MessageEncoder]] and [[MessageDecoder]] are what turn them into values, beside this rather than
 * inside it, so a claim with one unreadable message among ten is a thing a caller handles rather than a
 * shape this has to invent.
 */
trait QueueClient:

  /**
   * Hand a message to a key's queue.
   *
   * @param queue which queue to send to
   * @param message what to send, already written by an encoder
   * @return how deep the key is now; aborts with a [[ServiceError]] when the call does not land
   */
  def enqueue(queue: String, message: Message.Outgoing): IO[ServiceError, Enqueued]

  /**
   * Wait for a key's messages, and claim them.
   *
   * @param queue which queue to take from
   * @param maxWait how long to block for work, which the service may shorten to its own ceiling
   * @param maxBatch the most messages to claim at once, which the service may lower to its own ceiling
   * @return the claim, or `Idle` when the wait elapsed; aborts with a [[ServiceError]] when the call does
   *         not land
   */
  def dequeue(queue: String, maxWait: Duration, maxBatch: Int): IO[ServiceError, Dequeued]

  /**
   * Report what became of some of a claim's messages.
   *
   * @param receipt the handle the claim came with
   * @param verdicts what became of each message named; ids this claim does not own are ignored
   * @param retryAfter how long the key should wait before anything failed here is delivered again
   * @return whether it applied, or the claim was already revoked; aborts with a [[ServiceError]] when the
   *         call does not land
   */
  def settle(receipt: Receipt, verdicts: Chunk[Verdict], retryAfter: Duration): IO[ServiceError, Settled]

  /**
   * Renew every claim this consumer still holds, in one call.
   *
   * @param receipts everything it believes it holds; empty is legal
   * @return the new deadline, the span it runs for, and which of those receipts are no longer held;
   *         aborts with a [[ServiceError]] when the call does not land
   */
  def heartbeat(receipts: Chunk[Receipt]): IO[ServiceError, Renewed]


object QueueClient:

  /**
   * Dial a deployment, closed with the scope.
   *
   * @param endpoint where it answers
   * @return the client; aborts with `Failed` when the channel cannot be built
   */
  def scoped(endpoint: Endpoint): ZIO[Scope, ServiceError, QueueClient] =
    scoped(ZManagedChannel(channel(endpoint)), endpoint.patience)

  /**
   * Dial over a channel the caller built, for TLS, interceptors or an in-process transport.
   *
   * @param channel the channel to talk over, closed with the scope
   * @param patience how long a call may take beyond what it was asked to wait for
   * @return the client; aborts with `Failed` when the stub cannot be built
   */
  def scoped(channel: ZManagedChannel, patience: Duration = 10.seconds): ZIO[Scope, ServiceError, QueueClient] =
    KeyedQueueClient.scoped(channel).map(GrpcClient(_, patience)).mapError(dialling)

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
