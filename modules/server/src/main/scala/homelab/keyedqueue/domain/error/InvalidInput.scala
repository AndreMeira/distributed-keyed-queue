package homelab.keyedqueue.domain.error

import homelab.common.error.ValidationError


/**
 * One thing wrong with a request, named.
 *
 * '''A value, not an error.''' These are what validation *accumulates*; the error a caller eventually sees
 * is a single [[ValidationError]] carrying every one of them. Keeping them separate is what makes "one pass,
 * all the problems" expressible — an error type that could only ever hold one reason would force validation
 * to stop at the first.
 *
 * '''An implementation of the homelab's [[ValidationError.InvalidInput]].''' The contract is one problem
 * with a `message`; this enumerates the ones a queue can have. Sharing the contract is what lets
 * [[homelab.keyedqueue.domain.syntax.Validated]] be the toolkit's accumulating `Validation` rather than
 * something local.
 *
 * '''Named cases rather than free text.''' A case is something a test can assert on and a caller could one
 * day branch on; a string is something both have to pattern-match by eye. The wording lives in [[message]],
 * in one place, phrased for whoever reads the failed call.
 */
enum InvalidInput extends ValidationError.InvalidInput:

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
   * A settle named an empty id among the messages it reported on.
   */
  case EmptyDiscardId

  /**
   * A settle named the same id twice among the messages it reported on.
   *
   * Harmless to act on — settling an id already settled finds nothing — but a caller that sent one has
   * miscounted something, and hearing so is cheaper than wondering later why its numbers disagree.
   */
  case DuplicateDiscardId

  /**
   * A settle carried something that is not a receipt this service issued.
   *
   * Distinct from a claim that has been revoked, which is reported as `Stale` rather than refused: that one
   * is a race a correct consumer can lose — it was late — while this one is a string that was never a
   * receipt at all, and no amount of retrying makes it one.
   */
  case UnreadableReceipt

  /**
   * A settle named no messages at all.
   *
   * Refused rather than accepted as a no-op: it would decide nothing — the claim stays exactly as owed as
   * it was — while costing a round trip and reading, to whoever sent it, like something happened. A
   * consumer with nothing to report should send nothing, or a heartbeat if what it wants is to keep the
   * lease.
   */
  case EmptySettle

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
  override def message: String = this match
    case EmptyQueueName     => "a queue name is required"
    case EmptyMessageKey    => "a message key is required: it is what ordering is defined by"
    case EmptyMessageId     => "a message id is required: it is what a message is addressed by"
    case EmptyDiscardId     => "a discarded message must be named"
    case DuplicateDiscardId => "the same message was named twice to discard"
    case UnreadableReceipt  => "the receipt is not one this service issued"
    case EmptySettle        => "a settle must name at least one message"
    case NegativeMaxBatch   => "max_batch cannot be negative; zero or one means one message"
