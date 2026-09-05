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
 * its own digest and owns its own positions, so an adapter writes `scripts.produce.run(…)` with the
 * operation's real parameters and never touches a position again.
 *
 * @param produce appends a message and makes its key claimable
 * @param consume turns possession of a key into a claim
 * @param complete settles the in-flight message and decides the key's next state
 * @param heartbeat renews the claims a consumer still holds
 * @param watchdog the three repair sweeps
 */
final case class Scripts(
  produce: ProduceScript,
  consume: ConsumeScript,
  complete: CompleteScript,
  heartbeat: HeartbeatScript,
  watchdog: WatchdogScript,
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
      produce   <- ProduceScript.make
      consume   <- ConsumeScript.make
      complete  <- CompleteScript.make
      heartbeat <- HeartbeatScript.make
      watchdog  <- WatchdogScript.make
    yield Scripts(produce, consume, complete, heartbeat, watchdog)
