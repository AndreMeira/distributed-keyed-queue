package homelab.keyedqueue.demo


import com.google.protobuf.ByteString
import com.google.protobuf.duration.Duration as ProtoDuration
import homelab.keyedqueue.v1.*
import homelab.keyedqueue.v1.ZioKeyedQueueService.KeyedQueueClient
import io.grpc.ManagedChannelBuilder
import scalapb.zio_grpc.ZManagedChannel
import zio.*


/**
 * The four calls, over one channel — one instance's worth. [[Servers]] is how a scenario reaches several.
 *
 * Written against the published contract and nothing else — the demo module depends on
 * `distributed-keyed-queue-protocol-zio-grpc`, exactly as an outside consumer would, so anything awkward
 * here is awkward for a real caller too.
 *
 * @param stub the generated client
 */
final case class Client(stub: KeyedQueueClient):

  /**
   * Send one message.
   *
   * @param queue where to send it
   * @param key what orders it; messages sharing a key are worked one at a time
   * @param body the payload, as text
   * @return the key's depth after the append; fails when the call does
   */
  def enqueue(queue: String, key: String, body: String): Task[Long] =
    stub
      .enqueue(
        EnqueueRequest(
          queue,
          Some(
            Message(
              key = key,
              messageId = s"$key/$body",
              payloadType = "demo.Text/v1",
              encoding = "application/json",
              payload = ByteString.copyFromUtf8(body),
            )
          ),
        )
      )
      .map(_.keyDepth)

  /**
   * Claim a key's messages, blocking until there are some or the patience runs out.
   *
   * `head` being present is what says a claim was granted — an empty response is a quiet queue, not an
   * error, which is the contract's own rule and the one a consumer is most likely to get wrong.
   *
   * @param queue where to take from
   * @param patience how long to wait for work
   * @param batch the most messages to take at once
   * @return what was claimed, absent when nothing became claimable in time; fails when the call does
   */
  def dequeue(queue: String, patience: Duration, batch: Int = 1): Task[Option[Client.Claimed]] =
    stub
      .dequeue(DequeueRequest(queue, Some(ProtoDuration(patience.toSeconds)), batch))
      .map: response =>
        response.head.map: head =>
          Client.Claimed(response.receipt, head.messageId +: response.tail.map(_.messageId))

  /**
   * Report what became of every message in a claim.
   *
   * @param receipt what the claim answered with
   * @param ids the messages being settled
   * @param succeeded whether the handler finished them
   * @return whether it applied; false when the claim had already been revoked
   */
  def settle(receipt: String, ids: Seq[String], succeeded: Boolean): Task[Boolean] =
    stub
      .settle(
        SettleRequest(
          receipt = receipt,
          // No retryAfter: a failure here should come back at once, because watching it come back is the
          // point of the scenario that produces one.
          outcomes = ids.map(id =>
            MessageOutcome(id, if succeeded then Outcome.OUTCOME_DONE else Outcome.OUTCOME_FAILED)
          ),
        )
      )
      .map(_.applied == Applied.APPLIED_OK)


object Client:

  /**
   * One granted claim, reduced to what a settle needs.
   *
   * @param receipt what every settle for these messages names
   * @param ids the messages, in producer order
   */
  final case class Claimed(receipt: String, ids: Seq[String])

  /**
   * Dial an instance, closed with the scope.
   *
   * @param address where it answers, as `host:port`
   * @return the client
   */
  def scoped(address: String): ZIO[Scope, Throwable, Client] =
    val Array(host, port) = address.split(":"): @unchecked
    KeyedQueueClient
      .scoped(ZManagedChannel(ManagedChannelBuilder.forAddress(host, port.toInt).usePlaintext()))
      .map(Client.apply)
