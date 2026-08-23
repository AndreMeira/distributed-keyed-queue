package homelab.keyedqueue.domain.error


/**
 * One thing wrong with a request, named.
 *
 * '''A value, not an error.''' These are what validation *accumulates*; the error a caller eventually sees is
 * a single [[QueueError.InvalidRequest]] carrying every one of them. Keeping them separate is what makes "one
 * pass, all the problems" expressible — an error type that could only ever hold one reason would force
 * validation to stop at the first.
 *
 * '''Named cases rather than free text.''' A case is something a test can assert on and a caller could one
 * day branch on; a string is something both have to pattern-match by eye. The wording lives in [[message]],
 * in one place, phrased for whoever reads the failed call.
 */
enum InvalidInput:

  /** No queue was named. There is no default queue, and inventing one would silently misroute the message. */
  case EmptyQueueName

  /**
   * No key was given.
   *
   * Rejected rather than treated as "no ordering wanted": every message sharing one empty key would be
   * serialised behind the others, the opposite of what leaving it blank suggests.
   */
  case EmptyMessageKey

  /**
   * What to tell the caller.
   *
   * @return the problem, phrased in terms of the request
   */
  def message: String = this match
    case EmptyQueueName  => "a queue name is required"
    case EmptyMessageKey => "a message key is required: it is what ordering is defined by"
