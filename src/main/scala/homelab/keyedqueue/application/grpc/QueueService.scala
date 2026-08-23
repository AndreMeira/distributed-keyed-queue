package homelab.keyedqueue.application.grpc


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.service.usecase.*
import homelab.keyedqueue.infrastructure.codecs.grpc.Inbound.*
import homelab.keyedqueue.infrastructure.codecs.grpc.Outbound.*
import homelab.keyedqueue.v1.ZioKeyedQueue
import homelab.keyedqueue.v1
import io.grpc.{ Status, StatusException }
import io.scalaland.chimney.partial
import zio.*


/**
 * The gRPC surface: decode, run a use case, encode.
 *
 * There is no logic here on purpose. A handler that did more than translate would be a decision living in
 * the protocol adapter, where a second adapter — or a test — could not reach it.
 *
 * '''What is an error here, and what is a result.''' A dequeue that found nothing and a settle whose claim
 * was revoked are ordinary outcomes of an at-least-once queue, so they are fields in the response, where a
 * caller has to look at them. Only genuine faults become a `Status`, and the mapping lives in one place
 * rather than in each handler.
 *
 * @param acceptMessage accepts a message
 * @param claimMessage waits for one
 * @param settleMessage reports an outcome
 * @param renewClaims renews what a consumer holds
 */
final class QueueService(
  acceptMessage: EnqueueUseCase,
  claimMessage: DequeueUseCase,
  settleMessage: SettleUseCase,
  renewClaims: HeartbeatUseCase,
) extends ZioKeyedQueue.KeyedQueue:

  /**
   * Refuses a message that does not say how to read itself: the encoding is decoded here, not validated in
   * the use case, because the domain has no `Unspecified` to carry inwards.
   *
   * @param request the wire request
   * @return the wire response; aborts with `INVALID_ARGUMENT` when the message cannot be read
   */
  override def enqueue(request: v1.EnqueueRequest): IO[StatusException, v1.EnqueueResponse] =
    decode(request.toDomain).flatMap(acceptMessage(_).mapError(status)).map(_.toProto)

  /**
   * Blocks for the caller's `max_wait`, clamped by the use case. A timeout comes back as an absent
   * `delivery` rather than a status, so a quiet queue is not an error.
   *
   * @param request the wire request
   * @return the wire response, whose delivery is absent when nothing became ready
   */
  override def dequeue(request: v1.DequeueRequest): IO[StatusException, v1.DequeueResponse] =
    decode(request.toDomain).flatMap(claimMessage(_).mapError(status)).map(_.toProto)

  /**
   * A revoked claim answers `APPLIED_STALE` rather than failing: the caller must branch on it, and a status
   * would let a retrying client treat it as a transport problem.
   *
   * @param request the wire request
   * @return the wire response; aborts with `INVALID_ARGUMENT` when the outcome is unspecified
   */
  override def settle(request: v1.SettleRequest): IO[StatusException, v1.SettleResponse] =
    decode(request.toDomain).flatMap(settleMessage(_).mapError(status)).map(_.toProto)

  /**
   * Never fails on a receipt it cannot read — that one is reported stale alongside the genuinely revoked
   * ones, so a single bad handle does not cost a consumer the claims it still holds.
   *
   * @param request the wire request
   * @return the wire response, naming what the caller no longer holds
   */
  override def heartbeat(request: v1.HeartbeatRequest): IO[StatusException, v1.HeartbeatResponse] =
    decode(request.toDomain).flatMap(renewClaims(_).mapError(status)).map(_.toProto)

  /**
   * Turn a partial decode into a call that either proceeds or is refused.
   *
   * A wire message the domain cannot hold — no envelope, an unspecified encoding or outcome — is the
   * caller's to fix, so every such failure is `INVALID_ARGUMENT`, and all of them are reported at once
   * rather than the first.
   *
   * @param result what the transformer produced
   * @tparam A the domain request
   * @return the request; aborts with `INVALID_ARGUMENT` describing everything wrong with the message
   */
  private def decode[A](result: partial.Result[A]): IO[StatusException, A] =
    ZIO.fromEither(
      result.asEitherErrorPathMessageStrings.left
        .map(errors => reject(errors.map((path, reason) => s"$path: $reason").mkString("; ")))
    )

  /**
   * Refuse a call the caller must change to make work.
   *
   * @param reason what is wrong, in terms of the request rather than of the implementation
   * @return the status to fail with
   */
  private def reject(reason: String): StatusException =
    StatusException(Status.INVALID_ARGUMENT.withDescription(reason))

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
    case QueueError.Invalid(reason)          => reject(reason)
    case QueueError.StoreUnavailable(reason) => StatusException(Status.UNAVAILABLE.withDescription(reason))
    case QueueError.MalformedReply(reason)   => StatusException(Status.INTERNAL.withDescription(reason))
