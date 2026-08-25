package homelab.keyedqueue.infrastructure.redis.script


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.Namespace
import io.lettuce.core.ScriptOutputType
import zio.*


/**
 * One repair pass — `lua/watchdog.lua`.
 *
 * All three sweeps travel together — lapsed claims, dead workers, elapsed backoffs — so a pass is a single
 * round trip and a single blocking window on the server.
 *
 * `prefix` goes over as an argument because the sweep builds per-key names at runtime; it is the same tag
 * every key here shares, which is what keeps them in one cluster slot.
 *
 * @param sha the digest this script was loaded under
 * @param ns the queue to repair
 * @param limit the most entries to handle in one pass
 */
final case class WatchdogScript(sha: String, ns: Namespace, limit: Int) extends LuaScript[QueueStore.Swept]:

  /**
   * Multi, because a pass reports three lists.
   *
   * @return `MULTI`
   */
  override def output: ScriptOutputType = ScriptOutputType.MULTI

  /**
   * Everything the three sweeps read or repair: leases, key state, the ready list they push back onto,
   * fences they advance, worker liveness, and the backoff set.
   *
   * @return `claimed`, `state`, `ready`, `fence`, `workers`, `delayed`, in the order `lua/watchdog.lua`
   *         reads them
   */
  override def keys: Array[String] = Array(ns.claimed, ns.state, ns.ready, ns.fence, ns.workers, ns.delayed)

  /**
   * The per-pass cap, and the prefix the sweep rebuilds per-key names from.
   *
   * @return `limit`, `prefix`, in the order `lua/watchdog.lua` reads them
   */
  override def args: Array[Array[Byte]] = Array(LuaScript.utf8(limit.toString), LuaScript.utf8(ns.prefix))

  /**
   * Read the three lists the sweep reports, in the order the script returns them.
   *
   * @param value the raw reply
   * @return what the pass repaired; aborts with `MalformedReply` when the reply is not three lists
   */
  override def read(value: Any): IO[QueueError, QueueStore.Swept] = value match
    case values: java.util.List[?] if values.size == 3 =>
      ZIO.succeed(
        QueueStore.Swept(
          LuaScript.strings(values.get(0)).map(MessageKey.apply),
          LuaScript.strings(values.get(1)).map(WorkerId.apply),
          LuaScript.strings(values.get(2)).map(MessageKey.apply),
        )
      )
    case other                                         =>
      ZIO.fail(QueueError.MalformedReply(s"watchdog returned ${LuaScript.describe(other)}"))
