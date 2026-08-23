package homelab.keyedqueue.application.grpc


import com.google.protobuf.timestamp.Timestamp
import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.Claimed
import homelab.keyedqueue.domain.service.usecase.*
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.{ ClaimerPool, Watchdog }
import homelab.keyedqueue.v1.*
import io.grpc.{ Status, StatusException }
import zio.*

import java.time.Instant


/**
 * The gRPC surface: four unary calls over the use cases.
 *
 * '''What is an error here, and what is a result.''' A dequeue that found nothing and a settle whose lease
 * was revoked are ordinary outcomes of an at-least-once queue, so they are fields in the response —
 * `delivery` absent, `applied = APPLIED_STALE` — where a caller has to look at them. Only genuine faults
 * become a `Status`, and the mapping lives here rather than in each handler.
 *
 * @param enqueue accepts a message
 * @param dequeue waits for one
 * @param settle reports an outcome
 * @param heartbeat renews what a consumer holds
 * @param watchdog told about each queue served, so it gets swept
 */
final class QueueService(
  enqueue: EnqueueUseCase,
  dequeue: DequeueUseCase,
  settle: SettleUseCase,
  heartbeat: HeartbeatUseCase,
  watchdog: Watchdog,
) extends ZioKeyedQueue.KeyedQueue:

  override def enqueue(request: EnqueueRequest): IO[StatusException, EnqueueResponse] =
    val envelope = request.envelope.getOrElse(Envelope.defaultInstance)
    (for
      _     <- ZIO.fail(QueueError.Invalid("an encoding is required")).when(envelope.encoding.isEncodingUnspecified)
      queue  = QueueName(request.queue)
      _     <- watchdog.watch(queue)
      depth <- enqueue(queue, MessageKey(envelope.key), Chunk.fromArray(envelope.toByteArray))
    yield EnqueueResponse(depth)).mapError(status)

  override def dequeue(request: DequeueRequest): IO[StatusException, DequeueResponse] =
    val queue = QueueName(request.queue)
    (for
      _        <- watchdog.watch(queue)
      patience  = request.maxWait.fold(Duration.Zero)(window =>
                    Duration.fromSeconds(window.seconds) + Duration.fromNanos(window.nanos.toLong)
                  )
      claimed  <- dequeue(queue, patience)
    yield DequeueResponse(claimed.map(delivery))).mapError(status)

  override def settle(request: SettleRequest): IO[StatusException, SettleResponse] =
    val verdict    = if request.outcome.isOutcomeDone then Verdict.Done else Verdict.Failed
    val retryAfter = request.retryAfter.fold(Duration.Zero)(duration => Duration.fromSeconds(duration.seconds))
    (for
      _       <- ZIO
                   .fail(QueueError.Invalid("an outcome is required"))
                   .when(request.outcome.isOutcomeUnspecified)
      applied <- settle(request.receipt, verdict, retryAfter)
    yield SettleResponse(if applied then Applied.APPLIED_OK else Applied.APPLIED_STALE)).mapError(status)

  override def heartbeat(request: HeartbeatRequest): IO[StatusException, HeartbeatResponse] =
    heartbeat(Chunk.fromIterable(request.receipts))
      .map((until, stale) => HeartbeatResponse(stale, Some(timestamp(until))))
      .mapError(status)

  /**
   * Present a claim as the wire type, parsing the envelope back out of the stored bytes.
   *
   * @param claimed what the store handed over
   * @return the delivery a consumer receives
   */
  private def delivery(claimed: Claimed): Delivery =
    Delivery(
      receipt = claimed.claim.receipt,
      envelope = Some(Envelope.parseFrom(claimed.payload.toArray)),
      attempt = claimed.attempt,
      leaseExpiresAt = Some(timestamp(claimed.leaseExpiresAt)),
    )

  private def timestamp(instant: Instant): Timestamp =
    Timestamp(seconds = instant.getEpochSecond, nanos = instant.getNano)

  /**
   * Map a failure to a gRPC status.
   *
   * Bad input is the caller's to fix; anything else is ours, and transient by nature — the lease is the
   * backstop, so a caller that retries will be served.
   *
   * @param error what went wrong
   * @return the status to fail the call with
   */
  private def status(error: QueueError): StatusException = error match
    case QueueError.Invalid(reason)          => StatusException(Status.INVALID_ARGUMENT.withDescription(reason))
    case QueueError.StoreUnavailable(reason) => StatusException(Status.UNAVAILABLE.withDescription(reason))
    case QueueError.MalformedReply(reason)   => StatusException(Status.INTERNAL.withDescription(reason))
