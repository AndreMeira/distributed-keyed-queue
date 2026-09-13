package homelab.keyedqueue.infrastructure.redis.script.lock


import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.keys.LockKeys
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection
import io.lettuce.core.ScriptOutputType
import homelab.keyedqueue.infrastructure.redis.script.Codecs.given
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import zio.*


object ReleaseScript:

  type Input  = (keys: LockKeys, name: LockName, token: Token)
  type Output = Boolean

  /**
   * Register `lua/lock/release.lua` and hold it as the one call it makes.
   *
   * Integer, because the script answers whether it applied.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/lock/release.lua").map(LuaScript[Input, Output](_, ScriptOutputType.INTEGER))
