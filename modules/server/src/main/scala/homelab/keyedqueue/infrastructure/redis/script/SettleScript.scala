package homelab.keyedqueue.infrastructure.redis.script


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.model.{ Claim, Settlement }
import homelab.keyedqueue.domain.model.Settlement.Verdict
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.{ Connection, Namespace }
import io.lettuce.core.ScriptOutputType
import homelab.keyedqueue.infrastructure.redis.script.Codecs.given
import zio.*


object SettleScript:

  type Input  = (ns: Namespace, settlement: Settlement)
  type Output = Boolean

  /**
   * Register `lua/queue/settle.lua` and hold it as the one call it makes.
   *
   * Integer, because the script answers whether it applied.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/queue/settle.lua").map(LuaScript[Input, Output](_, ScriptOutputType.INTEGER))
