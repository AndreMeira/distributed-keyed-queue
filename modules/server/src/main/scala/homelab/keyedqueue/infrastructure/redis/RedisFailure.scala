package homelab.keyedqueue.infrastructure.redis

import homelab.common.error.ApplicationError


/**
 * What this adapter can fail with, defined where it is raised.
 *
 * '''Not in the domain.''' These are Redis's failure modes — a connection that cannot be opened, a reply
 * this code cannot read — and a port that named them would be describing one adapter's implementation to
 * every other. The domain's signatures say [[ApplicationError]]; what a failure *means* travels in the
 * toolkit's marker traits, which is what the protocol layer maps on.
 *
 * '''An `AdapterError` as a whole, not case by case.''' The port declares that it fails with an
 * `AdapterError` and nothing else — a store breaking is never the caller's fault and never a domain
 * condition — so this type has to satisfy that, which it can only do by carrying the marker itself. What
 * the cases add on top is the second question: a store that cannot be reached is transient and worth
 * retrying, while a reply that cannot be read is this code being wrong about its own scripts. They are
 * told apart by their markers rather than by name, so a second adapter's equivalents are classified
 * without anyone editing the mapping.
 */
enum RedisFailure extends ApplicationError.AdapterError:

  /** The substrate failed or could not be reached. Transient by nature: the lease is the backstop. */
  case Unavailable(reason: String) extends RedisFailure, ApplicationError.TransientError

  /** A script returned something this code does not know how to read — a defect, not a runtime condition. */
  case MalformedReply(reason: String) extends RedisFailure, ApplicationError.ImplementationError

  /**
   * What to tell a human. Phrased in terms of the queue rather than of Redis, because a reason can reach a
   * caller as a status description.
   *
   * @return the message
   */
  override def message: String = this match
    case Unavailable(reason)    => s"The queue store is unavailable: $reason"
    case MalformedReply(reason) => s"The queue store replied with something unreadable: $reason"
