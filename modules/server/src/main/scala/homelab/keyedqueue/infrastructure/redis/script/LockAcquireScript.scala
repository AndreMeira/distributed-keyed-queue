package homelab.keyedqueue.infrastructure.redis.script


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.LockClaim
import homelab.keyedqueue.domain.service.lock.LockStore.Hold
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection
import io.lettuce.core.ScriptOutputType
import homelab.keyedqueue.infrastructure.redis.script.Codecs.given
import zio.*

import java.time.Instant


object LockAcquireScript:

  type Input  = (name: LockName, ttl: Duration, patience: Duration)
  type Output = LockAcquireScript.Entered

  /**
   * What entering answered: the lock, or a place in its queue.
   *
   * Neither case names the lock — the script is told a name and answers about it — so turning a grant into
   * a hold belongs to the caller that asked.
   */
  enum Entered:

    /** The lock was free with nobody queued, and is now held under this token until the deadline. */
    case Granted(token: Token, leaseUntil: Instant)

    /** Queued: the ticket to ask with, and how long until the answer can next change. */
    case Queued(ticket: Long, recheck: Duration)

  /**
   * Register `lua/lock/acquire.lua` and hold it as the one call it makes.
   *
   * Multi, because the reply is `{granted, …}` in either shape.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/lock/acquire.lua").map(LuaScript[Input, Output](_, ScriptOutputType.MULTI))

  /** What entering answered: the lock, or a place in its queue. */
  enum Reply:

    /** The lock was free with nobody queued, and is now held. */
    case Granted(hold: Hold)

    /**
     * Queued: the ticket to ask with, and how long until the answer can next change.
     *
     * @param ticket the ticket's identity
     * @param recheck the delay after which granting could answer differently
     */
    case Queued(ticket: Long, recheck: Duration)
