package homelab.keyedqueue.infrastructure.redis.script.queue


import homelab.keyedqueue.domain.model.queue.ScheduledSubmission
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.infrastructure.redis.keys.ScheduleKeys
import homelab.keyedqueue.infrastructure.redis.script.Codecs.given
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import homelab.keyedqueue.infrastructure.redis.{ Connection, RedisFailure }
import io.lettuce.core.ScriptOutputType
import zio.*


object ScheduleScript:

  type Input  = (keys: ScheduleKeys, scheduled: ScheduledSubmission)
  type Output = QueueStore.Scheduled

  /**
   * Register `lua/queue/schedule.lua` and hold it as the one call it makes.
   *
   * Multi, because the reply is whether it was recorded and how long until it is due.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/queue/schedule.lua").map(LuaScript[Input, Output](_, ScriptOutputType.MULTI))
