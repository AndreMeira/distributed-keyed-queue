package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.infrastructure.redis.Connection.Commands
import homelab.keyedqueue.infrastructure.redis.script.*
import zio.*


/**
 * The five Lua scripts, loaded once and held as the calls they can make.
 *
 * Loading up front rather than on first use keeps the failure at startup, where a missing or unparseable
 * script is obvious, instead of on the first message. There is no `NOSCRIPT` fallback for the same reason a
 * connection needs no reconnect logic here: a Redis restart takes the connection with it, and the process
 * re-registers when it reconnects.
 *
 * '''It holds the five, and nothing else.''' Each script registers itself and carries its own digest, so
 * this is a place to reach them from rather than a layer they are called through.
 *
 * '''It hands out calls, not digests.''' A digest on its own is a string the caller must then pair with the
 * right keys and the right arguments, in the right order, from memory. Each script below already carries
 * its own digest and owns its own positions, so an adapter writes `scripts.enqueue.run(…)` with the
 * operation's real parameters and never touches a position again.
 *
 * @param enqueue appends a message and makes its key claimable
 * @param claim takes the next claimable key and hands over a batch of its messages
 * @param settle records what became of each message, and decides the key's next state
 * @param renew extends the claims a consumer still holds
 * @param sweep the two repair passes
 */
final case class Scripts(
  enqueue: LuaScript[EnqueueScript.Input, EnqueueScript.Output],
  claim: LuaScript[ClaimScript.Input, ClaimScript.Output],
  settle: LuaScript[SettleScript.Input, SettleScript.Output],
  renew: LuaScript[RenewScript.Input, RenewScript.Output],
  sweep: LuaScript[SweepScript.Input, SweepScript.Output],
)


object Scripts:

  /**
   * Register every script, so a missing or unparseable one fails at startup rather than on the first
   * message.
   *
   * Each script registers itself — it is the one place that knows which file it comes from.
   *
   * @return the calls they make; aborts with `RedisFailure` if one is missing or rejected
   */
  def make: ZIO[Commands, RedisFailure, Scripts] =
    for
      enqueue <- EnqueueScript.load
      claim   <- ClaimScript.load
      settle  <- SettleScript.load
      renew   <- RenewScript.load
      sweep   <- SweepScript.load
    yield Scripts(enqueue, claim, settle, renew, sweep)
