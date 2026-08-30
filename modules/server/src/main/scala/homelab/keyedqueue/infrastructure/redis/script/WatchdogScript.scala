package homelab.keyedqueue.infrastructure.redis.script


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.service.persistence.QueueStore
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.redis.Connection.Commands
import homelab.keyedqueue.infrastructure.redis.{ Connection, Namespace }
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
 * @param ref the digest this script was loaded under, from [[Scripts]]
 */
final class WatchdogScript(ref: LuaScript.Sha):

  /** Multi, because a pass reports three lists. */
  private val output: ScriptOutputType = ScriptOutputType.MULTI

  /**
   * Repair one queue.
   *
   * @param ns the queue to repair
   * @param limit the most entries to handle in one pass
   * @return what it repaired; aborts with `QueueError` when the store fails or the reply cannot be read
   */
  def run(ns: Namespace, limit: Int): ZIO[Connection.Commands, QueueError, QueueStore.Swept] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.evalsha[Any](ref, output, keys(ns), args(ns, limit)*))
        .mapError(LuaScript.failure)
        .flatMap(reply => ZIO.fromEither(read(reply)))

  /**
   * Everything the three sweeps read or repair: leases, key state, the ready list they push back onto,
   * fences they advance, worker liveness, and the backoff set.
   *
   * @param ns the queue to repair
   * @return `claimed`, `state`, `ready`, `fence`, `workers`, `delayed`, in the order `lua/watchdog.lua`
   *         reads them
   */
  private def keys(ns: Namespace): Array[String] =
    Array(ns.claimed, ns.state, ns.ready, ns.fence, ns.workers, ns.delayed)

  /**
   * The per-pass cap, and the prefix the sweep rebuilds per-key names from.
   *
   * @param ns the queue being repaired, for its prefix
   * @param limit the most entries to handle in one pass
   * @return `limit`, `prefix`, in the order `lua/watchdog.lua` reads them
   */
  private def args(ns: Namespace, limit: Int): Array[Array[Byte]] =
    Array(LuaScript.utf8(limit.toString), LuaScript.utf8(ns.prefix))

  /**
   * Read the three lists the sweep reports, in the order the script returns them.
   *
   * @param value the raw reply
   * @return what the pass repaired, or `MalformedReply` naming the element that could not be read
   */
  private def read(value: Any): Either[QueueError, QueueStore.Swept] =
    decoder.decode("watchdog", value)

  /**
   * The shape the script promises: `{reclaimed, recovered, released}`, each an array of bulk strings.
   *
   * All three arrive as strings and are given their types here, which is the last point at which a message
   * key and a worker id are still the same thing.
   *
   * @return the decoder
   */
  private def decoder: LuaScript.Decode.Of[QueueStore.Swept] =
    LuaScript.Decode.sized(3) {
      for
        reclaimed <- LuaScript.Decode.text.each.at(0)
        recovered <- LuaScript.Decode.text.each.at(1)
        released  <- LuaScript.Decode.text.each.at(2)
      yield QueueStore.Swept(
        reclaimed.map(MessageKey.apply),
        recovered.map(WorkerId.apply),
        released.map(MessageKey.apply),
      )
    }


object WatchdogScript:

  /**
   * Register `lua/watchdog.lua` and hold the digest it was given.
   *
   * @return the script, ready to run; aborts with `QueueError` if it is missing or the server rejects it
   */
  def make: ZIO[Connection.Commands, QueueError, WatchdogScript] =
    LuaScript.register("lua/watchdog.lua").map(WatchdogScript(_))
