package homelab.keyedqueue.infrastructure.redis.script.queue


import homelab.keyedqueue.infrastructure.redis.keys.BackoffKeys
import homelab.keyedqueue.infrastructure.redis.script.Codecs.given
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import homelab.keyedqueue.infrastructure.redis.{ Connection, RedisFailure }
import io.lettuce.core.ScriptOutputType
import zio.*


object BackoffScript:

  type Input = (keys: BackoffKeys, limit: Int)

  /**
   * What one partition's release did.
   *
   * @param released how many held-back keys had their backoff end
   * @param full whether the call hit its limit
   * @param nextDueIn how long until the partition's next backoff ends; absent when none is pending
   */
  final case class Output(released: Int, full: Boolean, nextDueIn: Option[Duration])

  /**
   * Register `lua/queue/backoff.lua` and hold it as the one call it makes.
   *
   * Multi, because the reply is three numbers.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/queue/backoff.lua").map(LuaScript[Input, Output](_, ScriptOutputType.MULTI))
