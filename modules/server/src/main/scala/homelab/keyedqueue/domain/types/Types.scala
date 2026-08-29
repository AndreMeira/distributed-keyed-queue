package homelab.keyedqueue.domain.types

/** The queue a message was addressed to. It is the address, never part of the message. */
type QueueName = QueueName.Type


object QueueName:
  opaque type Type <: String = String

  /**
   * A queue name, trusted.
   *
   * @param value the name as given
   * @return the queue name
   */
  def apply(value: String): Type = value


/** What a message is ordered by: one key is worked by one consumer at a time, keys run concurrently. */
type MessageKey = MessageKey.Type


object MessageKey:
  opaque type Type <: String = String

  /**
   * A key, trusted.
   *
   * @param value the key as given
   * @return the message key
   */
  def apply(value: String): Type = value


/** A claim's generation. A token authorises exactly one transition: grant, revoke and settle all advance it. */
type Token = Token.Type


object Token:
  opaque type Type <: Long = Long

  /**
   * A token, trusted.
   *
   * @param value the counter value the store handed out
   * @return the token
   */
  def apply(value: Long): Type = value


/** One idle connection's identity. Registered before it claims anything, expires if it stops beating. */
type WorkerId = WorkerId.Type


object WorkerId:
  opaque type Type <: String = String

  /**
   * A worker id, trusted.
   *
   * @param value the id
   * @return the worker id
   */
  def apply(value: String): Type = value


/** The opaque handle a consumer holds while it works a message: a [[Claim]] it cannot read. */
type ClaimRef = ClaimRef.Type


object ClaimRef:
  opaque type Type <: String = String

  /**
   * A reference, trusted.
   *
   * @param value the encoded handle
   * @return the reference
   */
  def apply(value: String): Type = value


/** What a consumer did with a message. */
enum Verdict:
  case Done, Failed


/**
 * How a payload is serialised.
 *
 * There is no `Unspecified` here, unlike on the wire: a message that does not say how to read it cannot be
 * acted on, so it is refused at the boundary rather than carried inwards as a state every later match has to
 * remember to reject.
 */
enum Encoding:
  case Json, Protobuf


/** Whether a call applied, or found the caller's claim already revoked. */
enum Applied:
  case Ok, Stale
