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
   * A message arrived without an id.
   *
   * Required because the store keys a key's queued messages by id: two under one id are one message, so an
   * empty id would collapse every unnamed message on that key into a single entry.
   */
  case EmptyMessageId

  /**
   * A settle named an empty id among the messages it wanted discarded.
   */
  case EmptyDiscardId

  /**
   * A settle named the same id twice among the messages it wanted discarded.
   *
   * Harmless to act on — dropping an id already gone finds nothing — but a caller that sent one has
   * miscounted something, and hearing so is cheaper than wondering later why its numbers disagree.
   */
  case DuplicateDiscardId

  /**
   * A dequeue asked for a negative batch size.
   *
   * Reachable because `uint32` on the wire decodes to a signed `Int`: a value at or above 2^31 arrives here
   * negative. Harmless further down — the use case clamps to at least one — but a caller that asked for
   * something impossible should hear so rather than silently receive one message.
   */
  case NegativeMaxBatch

  /**
   * What to tell the caller.
   *
   * @return the problem, phrased in terms of the request
   */
  def message: String = this match
    case EmptyQueueName     => "a queue name is required"
    case EmptyMessageKey    => "a message key is required: it is what ordering is defined by"
    case EmptyMessageId     => "a message id is required: it is what a message is addressed by"
    case EmptyDiscardId     => "a discarded message must be named"
    case DuplicateDiscardId => "the same message was named twice to discard"
    case NegativeMaxBatch   => "max_batch cannot be negative; zero or one means one message"
