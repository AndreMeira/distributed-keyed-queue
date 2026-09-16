package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.infrastructure.redis.keys.KeyLayout


/**
 * What this adapter can fail with, defined where it is raised.
 *
 * Redis's failure modes: a connection that cannot be opened, a reply this code cannot read. The enum
 * carries `AdapterError` as a whole, because a store breaking is never the caller's fault and never a
 * domain condition, and the ports it reaches declare [[ApplicationError]].
 *
 * What the cases add is the second question: a store that cannot be reached is transient and worth
 * retrying, while a reply that cannot be read is this code being wrong about its own scripts. They are
 * told apart by their markers rather than by name, which is what the protocol layer maps on.
 */
enum RedisFailure extends ApplicationError.AdapterError:

  /** The substrate failed or could not be reached. Transient by nature: the lease is the backstop. */
  case Unavailable(reason: String) extends RedisFailure, ApplicationError.TransientError

  /** A script returned something this code does not know how to read — a defect, not a runtime condition. */
  case MalformedReply(reason: String) extends RedisFailure, ApplicationError.ImplementationError

  case DecodingError(reason: String) extends RedisFailure, ApplicationError.DecodingError

  case PartitionConnectionMissing(partition: KeyLayout.Partition) extends RedisFailure, ApplicationError.ImplementationError

  /**
   * What to tell a human. Phrased in terms of the queue rather than of Redis, because a reason can reach a
   * caller as a status description.
   *
   * @return the message
   */
  override def message: String = this match
    case Unavailable(reason)              => s"The queue store is unavailable: $reason"
    case MalformedReply(reason)           => s"The queue store replied with something unreadable: $reason"
    case DecodingError(reason)            => s"Failed to decode Lua script output: $reason"
    case PartitionConnectionMissing(part) => s"No blocking connection was opened for partition $part"
