package homelab.keyedqueue.infrastructure.redis.script


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection
import io.lettuce.core.ScriptOutputType
import homelab.keyedqueue.infrastructure.redis.script.Codecs.given
import zio.*


object LockAbandonScript:

  type Input  = (name: LockName, ticket: Long)
  type Output = Boolean

  /**
   * Register `lua/lock/abandon.lua` and hold it as the one call it makes.
   *
   * Integer, because the script answers whether it withdrew anything.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/lock/abandon.lua").map(LuaScript[Input, Output](_, ScriptOutputType.INTEGER))
