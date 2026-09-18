package homelab.keyedqueue.domain.error

import homelab.common.error.ValidationError


/**
 * One thing wrong with a request, named.
 *
 * These are what validation accumulates; what a caller finally sees is a single [[ValidationError]]
 * carrying every one of them.
 */
enum InvalidInput extends ValidationError.InvalidInput:

  /** No queue was named. There is no default queue. */
  case EmptyQueueName

  /** No key was given. Every message sharing one empty key would be serialised behind the others. */
  case EmptyMessageKey

  /** A message arrived without an id. Two under one id, for one key, are one message. */
  case EmptyMessageId

  /** A message arrived without an encoding. Nothing can read a payload whose format is unstated. */
  case EmptyEncoding

  /** A settle named an empty id among the messages it reported on. */
  case EmptyDiscardId

  /** A settle named the same id twice among the messages it reported on. */
  case DuplicateDiscardId

  /**
   * A settle carried something that is not a receipt this service issued.
   *
   * Distinct from a claim that has been revoked, which comes back as `Stale` rather than refused: that is a
   * race a correct consumer can lose, this is a string that was never a receipt.
   */
  case UnreadableReceipt

  /**
   * A settle named no messages at all.
   *
   * A consumer with nothing to report should send nothing, or a heartbeat if what it wants is to keep the
   * lease.
   */
  case EmptySettle

  /** A dequeue asked for a negative batch size. */
  case NegativeMaxBatch

  /** No lock was named. There is no default lock. */
  case EmptyLockName

  /** A lock acquire or refresh asked for a hold of no time, or a negative one. */
  case NonPositiveTtl

  /**
   * A dequeue asked to wait for no time at all, or for a negative time.
   *
   * An absent `max_wait` lands here too: a caller must say how long it is prepared to wait.
   */
  case NonPositiveMaxWait

  /**
   * What to tell the caller.
   *
   * @return the problem, phrased in terms of the request
   */
  override def message: String = this match
    case EmptyQueueName     => "a queue name is required"
    case EmptyMessageKey    => "a message key is required: it is what ordering is defined by"
    case EmptyMessageId     => "a message id is required: it is what a message is addressed by"
    case EmptyEncoding      => "an encoding is required: it is how a consumer knows to read the payload"
    case EmptyDiscardId     => "a discarded message must be named"
    case DuplicateDiscardId => "the same message was named twice to discard"
    case UnreadableReceipt  => "the receipt is not one this service issued"
    case EmptySettle        => "a settle must name at least one message"
    case NegativeMaxBatch   => "max_batch cannot be negative; zero or one means one message"
    case NonPositiveMaxWait => "max_wait is required and must be greater than zero"
    case EmptyLockName      => "a lock name is required"
    case NonPositiveTtl     => "ttl is required and must be greater than zero"
