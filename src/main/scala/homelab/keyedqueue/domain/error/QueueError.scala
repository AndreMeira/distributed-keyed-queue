package homelab.keyedqueue.domain.error


import homelab.common.error.ApplicationError


/**
 * Everything this service can fail with, in the vocabulary the homelab uses everywhere else.
 *
 * Deliberately small. Most of what could go wrong in a queue is not an error at all — a dequeue that found
 * nothing, a settle whose lease was revoked — and those are *results*, carried in the response, so a caller
 * has to look at them. What is left is genuinely exceptional.
 */
enum QueueError extends ApplicationError:

  /** The substrate failed or could not be reached. Transient by nature: the lease is the backstop. */
  case StoreUnavailable(reason: String) extends QueueError, ApplicationError.AdapterError,
        ApplicationError.TransientError

  /** A script returned something this code does not know how to read — a defect, not a runtime condition. */
  case MalformedReply(reason: String) extends QueueError, ApplicationError.AdapterError,
        ApplicationError.ImplementationError

  /** The caller sent something the API cannot act on. */
  case Invalid(reason: String) extends QueueError, ApplicationError.DomainError

  override def message: String = this match
    case StoreUnavailable(reason) => s"The queue store is unavailable: $reason"
    case MalformedReply(reason)   => s"The queue store replied with something unreadable: $reason"
    case Invalid(reason)          => reason
