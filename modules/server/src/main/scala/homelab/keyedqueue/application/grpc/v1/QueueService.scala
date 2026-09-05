package homelab.keyedqueue.application.grpc.v1


import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.keyedqueue.domain.service.usecase.v1.*
import homelab.keyedqueue.infrastructure.codecs.grpc.v1.Inbound.*
import homelab.keyedqueue.infrastructure.codecs.grpc.v1.Outbound.*
import homelab.keyedqueue.v1
import homelab.keyedqueue.v1.ZioKeyedQueueService
import io.grpc.{ Status, StatusException }
import io.scalaland.chimney.partial
import zio.*


/**
 * The gRPC surface: decode, run a apply case, encode.
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
) extends ZioKeyedQueueService.KeyedQueue:

  /**
   * Refuses a message that does not say how to read itself: the encoding is decoded here, not validated in
   * the apply case, because the domain has no `Unspecified` to carry inwards.
   *
   * @param request the wire request
   * @return the wire response; aborts with `INVALID_ARGUMENT` when the message cannot be read
   */
  override def enqueue(request: v1.EnqueueRequest): IO[StatusException, v1.EnqueueResponse] =
    for
      safe     <- decoded(request.toDomain)
      response <- acceptMessage(safe).mapError(status)
    yield response.toProto

  /**
   * Blocks for the caller's `max_wait`, clamped by the apply case. A timeout comes back as an absent
   * `delivery` rather than a status, so a quiet queue is not an error.
   *
   * @param request the wire request
   * @return the wire response, whose delivery is absent when nothing became ready
   */
  override def dequeue(request: v1.DequeueRequest): IO[StatusException, v1.DequeueResponse] =
    for
      safe     <- decoded(request.toDomain)
      response <- claimMessage(safe).mapError(status)
    yield response.toProto

  /**
   * A revoked claim answers `APPLIED_STALE` rather than failing: the caller must branch on it, and a status
   * would let a retrying client treat it as a transport problem.
   *
   * @param request the wire request
   * @return the wire response; aborts with `INVALID_ARGUMENT` when the outcome is unspecified
   */
  override def settle(request: v1.SettleRequest): IO[StatusException, v1.SettleResponse] =
    for
      safe     <- decoded(request.toDomain)
      response <- settleMessage(safe).mapError(status)
    yield response.toProto

  /**
   * Never fails on a receipt it cannot read — that one is reported stale alongside the genuinely revoked
   * ones, so a single bad handle does not cost a consumer the claims it still holds.
   *
   * @param request the wire request
   * @return the wire response, naming what the caller no longer holds
   */
  override def heartbeat(request: v1.HeartbeatRequest): IO[StatusException, v1.HeartbeatResponse] =
    for
      safe     <- decoded(request.toDomain)
      response <- renewClaims(safe).mapError(status)
    yield response.toProto

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
  private def decoded[A](result: partial.Result[A]): IO[StatusException, A] =
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
   * '''By category, not by identity.''' The toolkit's marker traits already say what a failure means — the
   * caller sent something wrong, the trouble will pass, or it is ours to fix — and those are exactly the
   * three answers gRPC has room for. Matching on markers rather than on an enum of concrete errors means a
   * new adapter, or a new failure inside this one, is classified correctly without anyone remembering to
   * come back here, and the default is the safe one.
   *
   * A `ValidationError` describes *every* problem the request had, because `INVALID_ARGUMENT` is the
   * caller's cue to change something and one round trip per mistake is a poor way to learn what to change.
   * An unavailable store carries its reason, because a caller deciding whether to retry benefits from
   * knowing what is unreachable. Anything else is ours: the caller is told nothing beyond `INTERNAL`,
   * because the detail is for our logs and not for a stranger.
   *
   * @param error what went wrong
   * @return the status to fail the call with
   */
  private def status(error: ApplicationError): StatusException = error match
    case invalid: ValidationError           => reject(invalid.message)
    case _: ApplicationError.TransientError => StatusException(Status.UNAVAILABLE.withDescription(error.message))
    case _                                  => StatusException(Status.INTERNAL.withDescription(""))
