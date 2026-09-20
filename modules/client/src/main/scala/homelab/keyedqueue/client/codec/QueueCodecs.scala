package homelab.keyedqueue.client.codec


import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import homelab.keyedqueue.client.ServiceError
import homelab.keyedqueue.client.queue.*
import homelab.keyedqueue.client.queue.model.{ Claim, Dequeued, Enqueued, Message, MessageId, MessageKey, Receipt, Renewed, Settled, Verdict }
import homelab.keyedqueue.v1
import zio.*

import java.time.Instant


/**
 * Proto to Scala and back, for the queue's four calls.
 *
 * Pure: these are decisions about what arrived, so a caller lifts them where it needs an effect. Reading
 * in is partial, because the wire states a claim as a receipt and fields that mean something only when it
 * was granted, and an answer this client cannot hold is refused here rather than carried inwards.
 *
 * Payloads pass through as bytes. Turning them into values is a decoder's job, above this, which is what
 * keeps a batch with one unreadable message among ten out of the boundary's hands.
 */
private[client] object QueueCodecs:

  /**
   * A message on its way out, in the envelope the service expects.
   *
   * @param queue which queue it is addressed to
   * @param message what to send
   * @param sentAt when this client is sending it, which the service keeps and never reads
   * @return the wire request
   */
  def encode(queue: String, message: Message.Outgoing, sentAt: Instant): v1.EnqueueRequest =
    v1.EnqueueRequest(
      queue,
      Some(
        v1.Message(
          key = message.key,
          messageId = message.id,
          payloadType = message.payloadType,
          encoding = message.encoding,
          sentAt = Some(Timestamp(sentAt.getEpochSecond, sentAt.getNano)),
          payload = ByteString.copyFrom(message.payload.toArray),
        )
      ),
    )

  /**
   * What became of one message, as the wire states it.
   *
   * @param verdict which message, and what became of it
   * @return the wire outcome
   */
  def encode(verdict: Verdict): v1.MessageOutcome =
    v1.MessageOutcome(verdict.id, outcome(verdict.outcome))

  /**
   * How deep the key is now.
   *
   * @param response what the service answered
   * @return the depth it reported
   */
  def decode(response: v1.EnqueueResponse): Enqueued =
    Enqueued(response.keyDepth)

  /**
   * A claim, or the news that nothing became ready.
   *
   * @param response what the service answered
   * @return the answer, or `Unreadable` when a claim arrived without what owning it needs
   */
  def decode(response: v1.DequeueResponse): Either[ServiceError, Dequeued] =
    response.head match
      case None       => Right(Dequeued.Idle)
      case Some(head) =>
        for
          receipt <- claimed(response.receipt)
          first   <- message(head)
          rest    <- messages(response.tail)
          until   <- Protos.deadline(response.leaseExpiresAt, "a claim")
          ttl     <- Protos.span(response.leaseTtl, "a claim")
        yield Dequeued.Claimed(
          Claim(receipt, NonEmptyChunk.fromIterable(first, rest), until, ttl, response.backlogDepth)
        )

  /**
   * Whether the outcomes were recorded.
   *
   * @param response what the service answered
   * @return the answer, or `Unreadable` when it states neither
   */
  def decode(response: v1.SettleResponse): Either[ServiceError, Settled] =
    response.applied match
      case v1.Applied.APPLIED_OK    => Right(Settled.Applied)
      case v1.Applied.APPLIED_STALE => Right(Settled.Stale)
      case other                    =>
        Left(ServiceError.Unreadable(s"a settle answered ${other.name}, which says nothing about the claim"))

  /**
   * The new deadline, and what this caller no longer holds.
   *
   * @param response what the service answered
   * @return the answer, or `Unreadable` when the renewal states no deadline or no span
   */
  def decode(response: v1.HeartbeatResponse): Either[ServiceError, Renewed] =
    for
      until <- Protos.deadline(response.renewedUntil, "a heartbeat")
      ttl   <- Protos.span(response.leaseTtl, "a heartbeat")
    yield Renewed(Chunk.fromIterable(response.stale).map(Receipt.apply), until, ttl)

  /**
   * The receipt a granted claim is owned by.
   *
   * @param receipt what the answer carried
   * @return it, or `Unreadable` when a claim arrived with nothing to settle it by
   */
  private def claimed(receipt: String): Either[ServiceError, Receipt] =
    if receipt.isEmpty then Left(ServiceError.Unreadable("a claim arrived with no receipt, so nothing could settle it"))
    else Right(Receipt(receipt))

  /**
   * One delivery as a message this caller can work.
   *
   * @param delivery what the answer carried
   * @return the message, or `Unreadable` when the delivery carries none
   */
  private def message(delivery: v1.Delivery): Either[ServiceError, Message.Incoming] =
    for
      carried <- delivery.message.toRight(ServiceError.Unreadable("a delivery arrived with no message"))
      sentAt  <- Protos.deadline(carried.sentAt, "a delivery")
    yield Message.Incoming(
      key = MessageKey(carried.key),
      id = MessageId(delivery.messageId),
      payloadType = carried.payloadType,
      encoding = carried.encoding,
      payload = Chunk.fromArray(carried.payload.toByteArray),
      sentAt = sentAt,
      attempt = delivery.attempt,
    )

  /**
   * The rest of a batch, in the order it was handed over.
   *
   * @param deliveries what the answer carried after its head
   * @return them, or the first reason one of them could not be read
   */
  private def messages(deliveries: Seq[v1.Delivery]): Either[ServiceError, Chunk[Message.Incoming]] =
    deliveries.foldLeft[Either[ServiceError, Chunk[Message.Incoming]]](Right(Chunk.empty))(append)

  /**
   * One more message onto a batch being read, unless something has already failed to read.
   *
   * @param read what has been read so far, or why reading stopped
   * @param delivery the next delivery to read
   * @return the batch with it, or the first failure
   */
  private def append(
    read: Either[ServiceError, Chunk[Message.Incoming]],
    delivery: v1.Delivery,
  ): Either[ServiceError, Chunk[Message.Incoming]] =
    for
      sofar <- read
      next  <- message(delivery)
    yield sofar :+ next

  /**
   * What became of a message, as the wire names it.
   *
   * @param outcome what became of it
   * @return the wire form
   */
  private def outcome(outcome: Verdict.Outcome): v1.Outcome =
    outcome match
      case Verdict.Outcome.Done   => v1.Outcome.OUTCOME_DONE
      case Verdict.Outcome.Failed => v1.Outcome.OUTCOME_FAILED
