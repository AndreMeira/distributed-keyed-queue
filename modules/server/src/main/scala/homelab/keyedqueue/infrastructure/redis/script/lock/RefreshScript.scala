package homelab.keyedqueue.infrastructure.redis.script.lock


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection
import io.lettuce.core.ScriptOutputType
import homelab.keyedqueue.infrastructure.redis.script.Codecs.given
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import zio.*

import java.time.Instant


object RefreshScript:

  type Input  = (name: LockName, token: Token, ttl: Duration)
  type Output = (leaseUntil: Instant, renewed: Boolean)

  /**
   * Register `lua/lock/refresh.lua` and hold it as the one call it makes.
   *
   * Multi, because the reply is `{leaseUntil, ok}`.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/lock/refresh.lua").map(LuaScript[Input, Output](_, ScriptOutputType.MULTI))
