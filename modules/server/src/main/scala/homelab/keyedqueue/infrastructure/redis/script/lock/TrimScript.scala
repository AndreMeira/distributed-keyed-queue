package homelab.keyedqueue.infrastructure.redis.script.lock


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection
import io.lettuce.core.ScriptOutputType
import homelab.keyedqueue.infrastructure.redis.script.Codecs.given
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import zio.*


object TrimScript:

  type Input  = (partition: Int, grace: Duration, limit: Int)
  type Output = Chunk[LockName]

  /**
   * Register `lua/lock/trim.lua` and hold it as the one call it makes.
   *
   * Multi, because the reply is the names removed.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/lock/trim.lua").map(LuaScript[Input, Output](_, ScriptOutputType.MULTI))
