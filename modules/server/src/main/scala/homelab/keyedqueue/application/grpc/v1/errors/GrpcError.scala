package homelab.keyedqueue.application.grpc.v1.errors


import homelab.common.error.{ ApplicationError, ValidationError }
import homelab.keyedqueue.infrastructure.codecs.grpc.v1.Inbound
import io.grpc.{ Status, StatusException }


/**
 * The one place a failure becomes a gRPC status, for every service of the adapter.
 *
 * Applied once per service in [[homelab.keyedqueue.application.grpc.v1.Module]], with zio-grpc's `mapError`,
 * so a handler fails with the domain's own errors and never chooses a status itself.
 */
object GrpcError:

  /**
   * Map a failure to a gRPC status.
   *
   * By category, not by identity: the toolkit's marker traits say what a failure means — the caller sent
   * something wrong, the trouble will pass, or it is ours to fix — and those are the three answers gRPC has
   * room for. A new failure is therefore classified by the markers it carries, and the default is the safe
   * one.
   *
   * The one exception is a request that cannot be decoded, matched by its type: the `DecodingError` marker it
   * carries is shared with our own decoding defects (an unreadable store reply), which must stay `INTERNAL`.
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
  def fromApplicationError(error: ApplicationError): StatusException = error match
    case undecodable: Inbound.DecodingError => reject(undecodable.message)
    case invalid: ValidationError           => reject(invalid.message)
    case _: ApplicationError.TransientError => StatusException(Status.UNAVAILABLE.withDescription(error.message))
    case _                                  => StatusException(Status.INTERNAL.withDescription(""))

  /**
   * Refuse a call the caller must change to make work.
   *
   * @param reason what is wrong, in terms of the request rather than of the implementation
   * @return the status to fail with
   */
  private def reject(reason: String): StatusException =
    StatusException(Status.INVALID_ARGUMENT.withDescription(reason))
