package homelab.keyedqueue.infrastructure.redis.script.queue


import homelab.keyedqueue.domain.model.Claim
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.Connection.Commands
import homelab.keyedqueue.infrastructure.redis.Connection
import homelab.keyedqueue.infrastructure.redis.keys.QueueKeys
import io.lettuce.core.ScriptOutputType
import homelab.keyedqueue.infrastructure.redis.script.Codecs.given
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import zio.*

import java.time.Instant


object RenewScript:

  type Input  = (keys: QueueKeys, leaseTtl: Duration, held: Chunk[Claim])
  type Output = (renewedUntil: Instant, lost: Chunk[MessageKey])

  /**
   * Register `lua/queue/renew.lua` and hold it as the one call it makes.
   *
   * Multi, because the reply is a deadline and the keys it could not renew.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/queue/renew.lua").map(LuaScript[Input, Output](_, ScriptOutputType.MULTI))
