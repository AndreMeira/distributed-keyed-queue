package homelab.keyedqueue.client.queue.managed


import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.codec.{ Protos, QueueCodecs }
import homelab.keyedqueue.client.queue.QueueClient
import homelab.keyedqueue.client.queue.model.*
import homelab.keyedqueue.v1
import homelab.keyedqueue.v1.ZioKeyedQueueService.KeyedQueueClient
import io.grpc.StatusException
import zio.*


/**
 * The queue client over the generated stub.
 *
 * Every method is the same three steps — build the request, make the call, read the answer — so the
 * translation stays in one place and a reader comparing a method to its RPC sees nothing else.
 *
 * @param stub the generated client, already dialled
 */
final private[client] class GrpcClient(stub: KeyedQueueClient) extends QueueClient:

  /**
   * One `Enqueue` call, stamped with the moment it was sent.
   *
   * @param queue which queue to send to
   * @param message what to send
   * @return how deep the key is now; aborts with the [[ServiceError]] a transport failure amounts to
   */
  override def enqueue(queue: String, message: Message.Outgoing): IO[ServiceError, Enqueued] =
    for
      now    <- Clock.instant
      answer <- call(stub.enqueue(QueueCodecs.encode(queue, message, now)))
    yield QueueCodecs.decode(answer)

  /**
   * One `Dequeue` call, which blocks on the service until work arrives or the wait elapses.
   *
   * @param queue which queue to take from
   * @param maxWait how long to block
   * @param maxBatch the most messages to claim at once
   * @return the claim, or that nothing became ready; aborts with the [[ServiceError]] a transport failure
   *         amounts to
   */
  override def dequeue(queue: String, maxWait: Duration, maxBatch: Int): IO[ServiceError, Dequeued] =
    call(stub.dequeue(v1.DequeueRequest(queue, Some(Protos.encode(maxWait)), maxBatch)))
      .map(QueueCodecs.decode)
      .absolve

  /**
   * One `Settle` call, naming some or all of a claim's messages.
   *
   * @param receipt the handle the claim came with
   * @param verdicts what became of each message named
   * @param retryAfter how long the key should wait before anything failed here is delivered again
   * @return whether it applied; aborts with the [[ServiceError]] a transport failure amounts to
   */
  override def settle(
    receipt: Receipt,
    verdicts: Chunk[Verdict],
    retryAfter: Duration,
  ): IO[ServiceError, Settled] =
    call(stub.settle(v1.SettleRequest(receipt, Some(Protos.encode(retryAfter)), verdicts.map(QueueCodecs.encode))))
      .map(QueueCodecs.decode)
      .absolve

  /**
   * One `Heartbeat` call, renewing everything this consumer holds at once.
   *
   * @param receipts everything it believes it holds
   * @return the new deadline and what it no longer holds; aborts with the [[ServiceError]] a transport
   *         failure amounts to
   */
  override def heartbeat(receipts: Chunk[Receipt]): IO[ServiceError, Renewed] =
    call(stub.heartbeat(v1.HeartbeatRequest(receipts)))
      .map(QueueCodecs.decode)
      .absolve

  /**
   * Make one call, reporting a transport failure in this client's terms.
   *
   * @param rpc the call to make
   * @tparam A what it answers with
   * @return the answer; aborts with the [[ServiceError]] the failure amounts to
   */
  private def call[A](rpc: IO[StatusException, A]): IO[ServiceError, A] =
    rpc.mapError(Protos.failure)
