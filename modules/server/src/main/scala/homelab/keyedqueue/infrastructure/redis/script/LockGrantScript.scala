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


object LockGrantScript:

  type Input  = (name: LockName, ticket: Long, ttl: Duration)
  type Output = LockGrantScript.Asked

  /** What asking for a turn answered: the lock, a wait, or a queue that no longer knows the ticket. */
  enum Asked:

    /** The ticket was head and the lock free: it is now held under this token until the deadline. */
    case Granted(token: Token, leaseUntil: Instant)

    /** Not yet: the delay after which the answer can change. */
    case Wait(recheck: Duration)

    /** The ticket is not in the queue — expired and pruned, or withdrawn. */
    case Gone

  /**
   * Register `lua/lock/grant.lua` and hold it as the one call it makes.
   *
   * Multi, because the reply is `{granted, …}` in any of three shapes.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/lock/grant.lua").map(LuaScript[Input, Output](_, ScriptOutputType.MULTI))
