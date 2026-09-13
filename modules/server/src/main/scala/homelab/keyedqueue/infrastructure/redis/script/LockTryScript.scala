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


object LockTryScript:

  type Input   = (name: LockName, ttl: Duration)
  type Granted = (token: Token, leaseUntil: Instant)
  type Output  = Option[Granted]

  /**
   * Register `lua/lock/try.lua` and hold it as the one call it makes.
   *
   * Multi, because the reply is `{token, leaseUntil}` or nil.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/lock/try.lua").map(LuaScript[Input, Output](_, ScriptOutputType.MULTI))
