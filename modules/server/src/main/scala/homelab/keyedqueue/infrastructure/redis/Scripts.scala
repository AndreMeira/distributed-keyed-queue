package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ ClaimRef, Message }
import homelab.keyedqueue.domain.types.*
import io.lettuce.core.api.sync.RedisCommands
import zio.*

import java.nio.charset.StandardCharsets
import scala.io.Source


/**
 * The five Lua scripts, loaded once and called by digest.
 *
 * Loading up front rather than on first use keeps the failure at startup, where a missing or unparseable
 * script is obvious, instead of on the first message. There is no `NOSCRIPT` fallback for the same reason a
 * connection needs no reconnect logic here: a Redis restart takes the connection with it, and the process
 * re-registers when it reconnects.
 *
 * '''It hands out calls, not digests.''' A digest on its own is a string the caller must then pair with the
 * right keys and the right arguments, in the right order, from memory. Each method below pairs them once and
 * returns a [[LuaScript]] that already knows which digest it is, so an adapter names an operation and its
 * real parameters and never touches a position again.
 *
 * @param produceSha appends a message and makes its key claimable
 * @param consumeSha turns possession of a key into a claim
 * @param completeSha settles the in-flight message and decides the key's next state
 * @param heartbeatSha renews worker liveness and the claims still held
 * @param watchdogSha the three repair sweeps
 */
final case class Scripts(
  produceSha: String,
  consumeSha: String,
  completeSha: String,
  heartbeatSha: String,
  watchdogSha: String,
):

  /**
   * Append a message and make its key claimable.
   *
   * @param ns the queue to append in
   * @param message the message; the key it carries decides where it lands
   * @return the call
   */
  def produce(ns: Namespace, message: Message): ProduceScript =
    ProduceScript(produceSha, ns, message)

  /**
   * Finish a claim that `BLMOVE` already started.
   *
   * @param ns the queue being claimed from
   * @param worker the identity whose claiming list holds the key
   * @param key the key already taken
   * @param leaseTtl how long the resulting claim survives without a heartbeat
   * @return the call
   */
  def consume(ns: Namespace, worker: WorkerId, key: MessageKey, leaseTtl: Duration): ConsumeScript =
    ConsumeScript(consumeSha, ns, worker, key, leaseTtl)

  /**
   * Settle the in-flight message and decide the key's next state.
   *
   * @param claim the claim being settled, and the token that authorises it
   * @param verdict what became of the message
   * @param retryAfter how long to hold a failed message back; ignored when the verdict is `Done`
   * @return the call
   */
  def complete(claim: ClaimRef, verdict: Verdict, retryAfter: Duration): CompleteScript =
    CompleteScript(completeSha, claim, verdict, retryAfter)

  /**
   * Write a worker's liveness, and push forward the claims it names.
   *
   * @param ns the queue whose worker set to write to, and whose claims to renew
   * @param worker whose liveness is being written
   * @param leaseTtl how long the registration, and each renewed claim, survive without another beat
   * @param held the claims to renew alongside it, all in that queue; empty for a bare registration
   * @return the call
   */
  def heartbeat(ns: Namespace, worker: WorkerId, leaseTtl: Duration, held: Chunk[ClaimRef]): HeartbeatScript =
    HeartbeatScript(heartbeatSha, ns, worker, leaseTtl, held)

  /**
   * One repair pass over a queue.
   *
   * @param ns the queue to repair
   * @param limit the most entries to handle in one pass
   * @return the call
   */
  def watchdog(ns: Namespace, limit: Int): WatchdogScript =
    WatchdogScript(watchdogSha, ns, limit)


object Scripts:

  /**
   * Read the scripts from the classpath and register them with the server.
   *
   * @param redis the connection to register on
   * @return their digests; aborts with `QueueError` if one is missing or rejected
   */
  def make(redis: RedisCommands[String, Array[Byte]]): IO[QueueError, Scripts] =
    for {
      produce   <- register(redis, script = "produce")
      consume   <- register(redis, script = "consume")
      complete  <- register(redis, script = "complete")
      heartbeat <- register(redis, script = "heartbeat")
      watchdog  <- register(redis, script = "watchdog")
    } yield Scripts(produce, consume, complete, heartbeat, watchdog)

  /**
   * Read one script from `resources/lua`.
   *
   * @param name the file name without its extension
   * @return the script text; aborts if it is missing from the jar
   */
  private def read(name: String): IO[QueueError, String] =
    ZIO
      .attempt(Source.fromResource(s"lua/$name.lua").mkString)
      .mapError(error => QueueError.MalformedReply(s"lua/$name.lua is missing: ${error.getMessage}"))

  /**
   * Register one script and keep its digest.
   *
   * @param redis the connection to register on
   * @param script the script text
   * @return the digest to call it by; aborts with `QueueError` if the store rejects it
   */
  private def register(redis: RedisCommands[String, Array[Byte]], script: String): IO[QueueError, String] =
    read(script).flatMap: script =>
      ZIO
        .attemptBlocking(redis.scriptLoad(script.getBytes(StandardCharsets.UTF_8)))
        .mapError(error => QueueError.StoreUnavailable(s"loading a script failed: ${error.getMessage}"))
