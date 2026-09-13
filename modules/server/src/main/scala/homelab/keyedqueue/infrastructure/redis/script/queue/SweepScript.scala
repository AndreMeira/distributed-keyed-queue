package homelab.keyedqueue.infrastructure.redis.script.queue


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection.Commands
import homelab.keyedqueue.infrastructure.redis.{ Connection, QueueKeys }
import io.lettuce.core.ScriptOutputType
import homelab.keyedqueue.infrastructure.redis.script.Codecs.given
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import zio.*


object SweepScript:

  type Input  = (ns: QueueKeys, limit: Int)
  type Output = QueueStore.Swept

  /**
   * Register `lua/queue/sweep.lua` and hold it as the one call it makes.
   *
   * Multi, because the reply is the keys each of the two sweeps repaired.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/queue/sweep.lua").map(LuaScript[Input, Output](_, ScriptOutputType.MULTI))
