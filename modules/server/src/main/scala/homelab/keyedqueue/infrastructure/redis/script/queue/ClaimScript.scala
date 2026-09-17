package homelab.keyedqueue.infrastructure.redis.script.queue


import homelab.keyedqueue.domain.model.queue.{ Grant, Message }
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.keys.QueueKeys
import homelab.keyedqueue.infrastructure.redis.{ Connection, RedisFailure }
import io.lettuce.core.ScriptOutputType
import homelab.keyedqueue.infrastructure.redis.script.Codecs.given
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import zio.*

import java.time.Instant


object ClaimScript:

  type Input   = (keys: QueueKeys, leaseTtl: Duration, maxBatch: Int)
  type Claimed = (key: MessageKey, token: Token, deadline: Instant, backlog: Int, batch: NonEmptyChunk[Grant.Owned])
  type Output  = Option[Claimed]

  /**
   * Register `lua/queue/claim.lua` and hold it as the one call it makes.
   *
   * Multi, because a claim comes back as three numbers and three arrays — a reply's shape on the wire is
   * not a function of what it decodes to, so it is said here rather than derived.
   *
   * @return the script, ready to run; aborts with `RedisFailure` if it is missing or the server rejects it
   */
  def load: ZIO[Connection.Commands, RedisFailure, LuaScript[Input, Output]] =
    LuaScript.register("lua/queue/claim.lua").map(LuaScript[Input, Output](_, ScriptOutputType.MULTI))

  object Output:

    /**
     * Line the three parallel arrays back up into one message each.
     *
     * Bounded by the shortest of the three, so a reply whose arrays disagree in length yields what they agree
     * on rather than throwing.
     *
     * @param ids the message ids, in producer order
     * @param messages their payloads, in the same order
     * @param attempts their delivery counts, in the same order
     * @return one entry per message
     */
    def owned(ids: Chunk[String], messages: Chunk[Message], attempts: Chunk[Int]): Chunk[Grant.Owned] =
      val size = ids.size.min(messages.size).min(attempts.size)
      Chunk
        .fromIterable(0 until size)
        .map(index => Grant.Owned(MessageId(ids(index)), messages(index), attempts(index)))
