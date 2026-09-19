package homelab.keyedqueue.client


/**
 * What a call to the lock service aborts with.
 *
 * The client is the edge, so a transport's exception is wrapped here and never reaches a caller's
 * signature. The three cases are the three decisions a caller makes: fix the request, retry later, or give
 * up and report.
 */
enum LockError:

  /**
   * The service refused the request as malformed.
   *
   * @param message what it said was wrong
   */
  case Rejected(message: String)

  /**
   * The deployment did not answer.
   *
   * @param cause what the transport raised
   */
  case Unreachable(cause: Throwable)

  /**
   * The service answered with something this client cannot hold — a grant with no lease, say.
   *
   * @param message which part of the answer was unusable
   */
  case Unreadable(message: String)

  /**
   * The call failed for a reason this client does not read.
   *
   * @param cause what the transport raised
   */
  case Failed(cause: Throwable)
