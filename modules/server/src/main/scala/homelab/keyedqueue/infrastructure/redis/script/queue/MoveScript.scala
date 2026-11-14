package homelab.keyedqueue.infrastructure.redis.script.queue


import homelab.keyedqueue.infrastructure.redis.keys.ScheduleKeys
import homelab.keyedqueue.infrastructure.redis.script.Codecs.given
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import homelab.keyedqueue.infrastructure.redis.{ Connection, RedisFailure }
import io.lettuce.core.ScriptOutputType
import zio.*


object MoveScript:

  type Input = (keys: ScheduleKeys, limit: Int)

  /**
   * What one partition's move did.
   *
   * @param moved how many due messages were appended to their key's line
   * @param discarded how many were dropped because their id was still queued for the key
   * @param full whether the call hit its limit
   * @param nextDueIn how long until the partition's next entry falls due; absent when none is scheduled
   */
  final case class Output(moved: Int, discarded: Int, full: Boolean, nextDueIn: Option[Duration])

  /**
   * Register `lua/queue/move.lua` and hold it as the one call it makes.
   *
   * Multi, because the reply is four numbers.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/queue/move.lua").map(LuaScript[Input, Output](_, ScriptOutputType.MULTI))
