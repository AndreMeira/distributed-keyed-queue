package homelab.keyedqueue.infrastructure.redis.script.queue


import homelab.keyedqueue.domain.model.queue.Message
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection.Commands
import homelab.keyedqueue.infrastructure.redis.Connection
import homelab.keyedqueue.infrastructure.redis.keys.QueueKeys
import io.lettuce.core.ScriptOutputType
import homelab.keyedqueue.infrastructure.redis.script.Codecs.given
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import zio.*


object EnqueueScript:

  type Input  = (keys: QueueKeys, message: Message)
  type Output = Long

  /**
   * Register `lua/queue/enqueue.lua` and hold it as the one call it makes.
   *
   * Integer, because the script's last act is an `LLEN`.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/queue/enqueue.lua").map(LuaScript[Input, Output](_, ScriptOutputType.INTEGER))
