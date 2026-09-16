package homelab.keyedqueue.infrastructure.redis.script


import homelab.keyedqueue.infrastructure.redis.Connection.Commands
import homelab.keyedqueue.infrastructure.redis.RedisFailure
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import homelab.keyedqueue.infrastructure.redis.script.queue.*
import zio.*


/**
 * The five Lua scripts, loaded once and held as the calls they can make.
 *
 * Loading up front rather than on first use keeps the failure at startup, where a missing or unparseable
 * script is obvious, instead of on the first message. There is no `NOSCRIPT` fallback for the same reason a
 * connection needs no reconnect logic here: a Redis restart takes the connection with it, and the process
 * re-registers when it reconnects.
 *
 * It holds the five and nothing else: each script registers itself, carries its own digest and owns its
 * own argument positions, so an adapter writes `scripts.enqueue.run(…)` with the operation's real
 * parameters and never handles a digest or a position.
 *
 * @param enqueue appends a message and makes its key claimable
 * @param claim takes the next claimable key and hands over a batch of its messages
 * @param settle records what became of each message, and decides the key's next state
 * @param renew extends the claims a consumer still holds
 * @param sweep the two repair passes
 */
final case class QueueScripts(
  enqueue: LuaScript[EnqueueScript.Input, EnqueueScript.Output],
  claim: LuaScript[ClaimScript.Input, ClaimScript.Output],
  settle: LuaScript[SettleScript.Input, SettleScript.Output],
  renew: LuaScript[RenewScript.Input, RenewScript.Output],
  sweep: LuaScript[SweepScript.Input, SweepScript.Output],
)


object QueueScripts:

  /**
   * Register every script, so a missing or unparseable one fails at startup rather than on the first
   * message.
   *
   * Each script registers itself — it is the one place that knows which file it comes from.
   *
   * @return the calls they make; aborts with `RedisFailure` if one is missing or rejected
   */
  def make: ZIO[Commands, RedisFailure, QueueScripts] =
    for
      enqueue <- EnqueueScript.load
      claim   <- ClaimScript.load
      settle  <- SettleScript.load
      renew   <- RenewScript.load
      sweep   <- SweepScript.load
    yield QueueScripts(enqueue, claim, settle, renew, sweep)
