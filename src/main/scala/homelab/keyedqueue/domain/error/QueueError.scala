package homelab.keyedqueue.domain.error


import homelab.common.error.ApplicationError
import zio.NonEmptyChunk


/**
 * Everything this service can fail with, in the vocabulary the homelab uses everywhere else.
 *
 * Deliberately small. Most of what could go wrong in a queue is not an error at all — a dequeue that found
 * nothing, a settle whose lease was revoked — and those are *results*, carried in the response, so a caller
 * has to look at them. What is left is genuinely exceptional.
 */
enum QueueError extends ApplicationError:

  /** The substrate failed or could not be reached. Transient by nature: the lease is the backstop. */
  case StoreUnavailable(reason: String) extends QueueError, ApplicationError.AdapterError, ApplicationError.TransientError

  /** A script returned something this code does not know how to read — a defect, not a runtime condition. */
  case MalformedReply(reason: String) extends QueueError, ApplicationError.AdapterError, ApplicationError.ImplementationError

  /**
   * The caller sent something the API cannot act on — every reason it cannot, not merely the first.
   *
   * Carrying a `NonEmptyChunk` is what makes accumulating validation worth doing: a caller that sent two bad
   * fields learns about both in one round trip instead of fixing one and being told about the next.
   */
  case InvalidRequest(problems: NonEmptyChunk[InvalidInput]) extends QueueError, ApplicationError.DomainError

  /**
   * The service's own configuration is unusable.
   *
   * Deliberately not [[InvalidRequest]], though both are "something is invalid": this one is nobody's request
   * and no caller can fix it. Conflating them once meant a malformed `queue.conf` was reported to callers as
   * `INVALID_ARGUMENT`, blaming them for our file.
   */
  case Misconfigured(reason: String) extends QueueError, ApplicationError.AdapterError, ApplicationError.UnrecoverableError

  /** The service could not start. Nothing is running, so there is nothing to retry against. */
  case StartupFailed(reason: String) extends QueueError, ApplicationError.AdapterError, ApplicationError.UnrecoverableError

  /**
   * What to tell a human. Reasons are phrased in terms of the queue rather than of Redis, because this is
   * what reaches a caller as a status description.
   *
   * @return the message
   */
  override def message: String = this match
    case StoreUnavailable(reason) => s"The queue store is unavailable: $reason"
    case MalformedReply(reason)   => s"The queue store replied with something unreadable: $reason"
    case InvalidRequest(problems) => problems.map(_.message).mkString("; ")
    case Misconfigured(reason)    => s"The service is misconfigured: $reason"
    case StartupFailed(reason)    => s"The service could not start: $reason"
