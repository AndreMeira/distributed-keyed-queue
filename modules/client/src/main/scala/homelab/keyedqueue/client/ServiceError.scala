package homelab.keyedqueue.client

import homelab.common.error.ApplicationError


/**
 * What a call to the service aborts with, whichever half of it was called.
 *
 * The client is the edge, so a transport's exception is wrapped here and never reaches a caller's
 * signature. The four cases are three decisions: fix the request, retry later, or give up and report — the
 * last of which covers both an answer this client cannot read and a failure it does not recognise.
 *
 * An adapter error, because that is what a deployment on the other side of a wire is to the code calling
 * it: a consumer built on these reports failures the way every other one in the homelab does.
 */
enum ServiceError extends ApplicationError.AdapterError:

  /**
   * The service refused the request as malformed.
   *
   * @param reason what it said was wrong
   */
  case Rejected(reason: String)

  /**
   * The deployment did not answer.
   *
   * @param cause what the transport raised
   */
  case Unreachable(cause: Throwable)

  /**
   * The service answered with something this client cannot hold — a grant with no lease, say.
   *
   * @param reason which part of the answer was unusable
   */
  case Unreadable(reason: String)

  /**
   * The call failed for a reason this client does not read.
   *
   * @param cause what the transport raised
   */
  case Failed(cause: Throwable)

  /** @return what went wrong, for a log or a report */
  override def message: String = this match
    case Rejected(reason)   => s"the service refused the request: $reason"
    case Unreachable(cause) => s"the deployment did not answer: ${cause.getMessage}"
    case Unreadable(reason) => s"the service answered with something unusable: $reason"
    case Failed(cause)      => s"the call failed: ${cause.getMessage}"
