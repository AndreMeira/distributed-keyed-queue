package homelab.keyedqueue.application.grpc.v1


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.keyedqueue.domain.service.usecase.queue.*
import homelab.keyedqueue.infrastructure.codecs.grpc.v1.Inbound.*
import homelab.keyedqueue.infrastructure.codecs.grpc.v1.Outbound.*
import homelab.keyedqueue.v1
import homelab.keyedqueue.v1.ZioKeyedQueueService
import zio.*


/**
 * The gRPC surface: decode, run a use case, encode.
 *
 * There is no logic here on purpose. A handler that did more than translate would be a decision living in
 * the protocol adapter, where a second adapter — or a test — could not reach it.
 *
 * A dequeue that found nothing and a settle whose claim was revoked are ordinary outcomes of an
 * at-least-once queue, so they are fields in the response where a caller has to look at them. Only genuine
 * faults fail the call, with the domain's own errors: [[Module]] turns them into a `Status` once for the whole
 * service ([[errors.GrpcError]]), so no handler chooses one.
 *
 * Hence the generic `GKeyedQueue[Any, ApplicationError]` rather than the generated `KeyedQueue`, whose methods
 * must already fail with a `StatusException`. Its methods take a call context, unused here.
 *
 * Every RPC is measured here, under the name of the RPC: a handler is the whole of one call — decode, use
 * case, encode — so what is timed is what the caller experienced. See
 * `docs/architecture/observability.md`.
 *
 * @param monitor what each RPC is counted and timed against
 * @param useCases the operations this surface exposes, one per RPC
 */
final class QueueService(
  monitor: Monitor,
  useCases: QueueUseCases,
) extends ZioKeyedQueueService.GKeyedQueue[Any, ApplicationError]:

  /**
   * Refuses an enqueue with no message: a `v1.EnqueueRequest` whose `message` is absent cannot be read into
   * the domain, so it is refused here rather than in the use case, which has no absent message to carry.
   *
   * @param request the `v1.EnqueueRequest`
   * @param context the call's context, unused
   * @return the `v1.EnqueueResponse`; aborts with `DecodingError` when the message is absent or its `sent_at`
   *         is out of protobuf's range, and with the use case's errors
   */
  override def enqueue(request: v1.EnqueueRequest, context: Any): IO[ApplicationError, v1.EnqueueResponse] =
    monitor.measure("QueueService.enqueue"):
      for
        safe     <- ZIO.fromEither(request.toDomain)
        response <- useCases.enqueue(safe)
      yield response.toProto

  /**
   * Blocks for the caller's `max_wait`, clamped by the use case. A timeout comes back as an absent
   * `delivery` rather than a status, so a quiet queue is not an error.
   *
   * @param request the `v1.DequeueRequest`
   * @param context the call's context, unused
   * @return the `v1.DequeueResponse`, whose delivery is absent when nothing became ready; aborts with
   *         `DecodingError` when `max_wait` is out of protobuf's range, and with the use case's errors
   */
  override def dequeue(request: v1.DequeueRequest, context: Any): IO[ApplicationError, v1.DequeueResponse] =
    monitor.measure("QueueService.dequeue"):
      for
        safe     <- ZIO.fromEither(request.toDomain)
        response <- useCases.dequeue(safe)
      yield response.toProto

  /**
   * A revoked claim answers `APPLIED_STALE` rather than failing: the caller must branch on it, and a status
   * would let a retrying client treat it as a transport problem.
   *
   * @param request the `v1.SettleRequest`
   * @param context the call's context, unused
   * @return the `v1.SettleResponse`; aborts with `DecodingError` when an outcome is unspecified or
   *         `retry_after` is out of protobuf's range, and with the use case's errors
   */
  override def settle(request: v1.SettleRequest, context: Any): IO[ApplicationError, v1.SettleResponse] =
    monitor.measure("QueueService.settle"):
      for
        safe     <- ZIO.fromEither(request.toDomain)
        response <- useCases.settle(safe)
      yield response.toProto

  /**
   * Records a message to be appended to its key's line after `deliver_after`. A schedule repeating an id still
   * scheduled is not an error: it answers `scheduled = false`, with the earlier schedule's remaining time.
   *
   * @param request the `v1.ScheduleRequest`
   * @param context the call's context, unused
   * @return the `v1.ScheduleResponse`; aborts with `DecodingError` when the message is absent or a duration or
   *         timestamp is out of protobuf's range, and with the use case's errors (a `ValidationError` for a delay not greater than zero or beyond the horizon)
   */
  override def schedule(request: v1.ScheduleRequest, context: Any): IO[ApplicationError, v1.ScheduleResponse] =
    monitor.measure("QueueService.schedule"):
      for
        safe     <- ZIO.fromEither(request.toDomain)
        response <- useCases.schedule(safe)
      yield response.toProto

  /**
   * Never fails on a receipt it cannot read — that one is reported stale alongside the genuinely revoked
   * ones, so a single bad handle does not cost a consumer the claims it still holds.
   *
   * @param request the `v1.HeartbeatRequest`
   * @param context the call's context, unused
   * @return the `v1.HeartbeatResponse`, naming what the caller no longer holds; aborts with the use case's
   *         errors
   */
  override def heartbeat(request: v1.HeartbeatRequest, context: Any): IO[ApplicationError, v1.HeartbeatResponse] =
    monitor.measure("QueueService.heartbeat"):
      for
        safe     <- ZIO.fromEither(request.toDomain)
        response <- useCases.heartbeat(safe)
      yield response.toProto
