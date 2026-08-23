package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.model.{ ClaimRef, Claimed }
import homelab.keyedqueue.domain.types.*
import homelab.keyedqueue.infrastructure.codecs.storage.StoredMessage
import io.lettuce.core.api.sync.RedisCommands
import io.lettuce.core.{ LMoveArgs, ScriptOutputType }
import zio.*

import java.time.Instant


/**
 * One blocking connection, and the identity that makes its death recoverable.
 *
 * `BLMOVE` occupies its connection for the whole wait, so a claimer is exclusive to one caller at a time —
 * which is why they are pooled rather than created per request.
 *
 * '''It registers before it claims, in the queue it is claiming from.''' Worker liveness lives per queue, so
 * registering somewhere else is as bad as not registering at all: the sweep that would recover this
 * connection's in-transition keys reads the queue's own worker set and would never see it. Registration is
 * therefore per namespace, done once on first use and kept alive by [[beat]].
 *
 * @param redis this claimer's own connection
 * @param scripts the loaded script digests
 * @param worker this connection's identity
 * @param leaseTtl how long a claim, and this claimer's own registration, survive without a heartbeat
 * @param known the queues it has registered in
 */
final class Claimer(
  redis: RedisCommands[String, Array[Byte]],
  scripts: Scripts,
  worker: WorkerId,
  leaseTtl: Duration,
  known: Ref[Set[QueueName]],
):

  /**
   * Wait up to `timeout` for a key to become claimable, then take its oldest message.
   *
   * The whole call is one uninterruptible region with only the wait restored, and it releases on
   * interruption: a cancelled caller cannot un-send a network call, so the key may already have moved
   * server-side. Worker liveness is the backstop for a crash, but a live process never expires, so it has to
   * clean up after itself.
   *
   * @param queue the queue to claim from
   * @param timeout how long to wait for work
   * @return the claim, or `None` when nothing became claimable in time; aborts with `QueueError` when the
   *         store fails
   */
  def claim(queue: QueueName, timeout: Duration): IO[QueueError, Option[Claimed]] =
    val ns = Namespace(queue)
    register(ns) *> ZIO
      .uninterruptibleMask: restore =>
        restore(take(ns, timeout)).flatMap:
          case None      => ZIO.none
          case Some(key) => granted(ns, MessageKey(key))
      .onInterrupt(release(ns))

  /**
   * Renew this claimer's registration wherever it has one.
   *
   * An idle claimer must keep beating: registration lapses on silence, and a claim made after that would be
   * born unrecoverable.
   *
   * @return noop; a failed beat is ignored because the next one is moments away
   */
  def beat: UIO[Unit] =
    known.get.flatMap(ZIO.foreachDiscard(_)(queue => renew(Namespace(queue), Chunk.empty).ignore))

  /**
   * Announce this connection in a queue, once.
   *
   * @param ns the queue to register in
   * @return noop; aborts with `QueueError` when the store fails
   */
  private def register(ns: Namespace): IO[QueueError, Unit] =
    ZIO
      .unlessZIO(known.get.map(_.contains(ns.queue)))(
        renew(ns, Chunk.empty) *> known.update(_ + ns.queue)
      )
      .unit

  /**
   * Write this claimer's liveness, and push forward any claims named.
   *
   * @param ns the queue whose worker set to write to
   * @param held claims to renew alongside the registration
   * @return the keys that could not be renewed; aborts with `QueueError` when the store fails
   */
  private def renew(ns: Namespace, held: Chunk[ClaimRef]): IO[QueueError, Chunk[String]] =
    val pairs = held.flatMap(claim => Chunk(Lua.utf8(claim.key), Lua.utf8(claim.token.toString)))
    Lua
      .call(
        redis,
        scripts.heartbeat,
        ScriptOutputType.MULTI,
        Array(ns.claimed, ns.fence, ns.workers),
        (Chunk(Lua.utf8(leaseTtl.toMillis.toString), Lua.utf8(worker)) ++ pairs).toArray,
      )
      .flatMap:
        case values: java.util.List[?] if values.size == 2 => ZIO.succeed(Lua.strings(values.get(1)))
        case other                                         => ZIO.fail(QueueError.MalformedReply(s"heartbeat returned ${Lua.describe(other)}"))

  /**
   * Take a claimable key into this connection's own claiming list.
   *
   * @param ns the queue to take from
   * @param timeout how long to wait
   * @return the key, or `None` on timeout
   */
  private def take(ns: Namespace, timeout: Duration): IO[QueueError, Option[String]] =
    ZIO
      .attemptBlocking(
        Option(
          redis.blmove(ns.ready, ns.claiming(worker), LMoveArgs.Builder.leftRight(), timeout.toMillis.toDouble / 1000.0)
        )
      )
      .mapBoth(Lua.failure, _.map(Lua.text))

  /**
   * Turn possession of a key into a claim: message in hand, lease running, token issued.
   *
   * The stored bytes are read back here, so an unreadable message fails the claim rather than travelling one
   * layer further as bytes nobody above this adapter should have to think about.
   *
   * @param ns the queue
   * @param key the key taken by [[take]]
   * @return the claim, or `None` when the key turned out to have nothing left; aborts with `MalformedReply`
   *         when the reply, or the message in it, cannot be read
   */
  private def granted(ns: Namespace, key: MessageKey): IO[QueueError, Option[Claimed]] =
    Lua
      .call(
        redis,
        scripts.consume,
        ScriptOutputType.MULTI,
        Array(ns.claiming(worker), ns.state, ns.claimed, ns.fence, ns.msgs(key), ns.inflight(key), ns.attempts),
        Array(Lua.utf8(key), Lua.utf8(leaseTtl.toMillis.toString)),
      )
      .flatMap:
        case null                                          => ZIO.none
        case values: java.util.List[?] if values.isEmpty   => ZIO.none
        case values: java.util.List[?] if values.size == 4 =>
          val payload = values.get(0) match
            case bytes: Array[Byte] => Chunk.fromArray(bytes)
            case _                  => Chunk.empty[Byte]
          List(1, 2, 3).map(values.get).collect { case number: java.lang.Long => number.longValue } match
            case List(token, attempt, deadline) =>
              StoredMessage
                .fromBytes(payload)
                .map(message => Some(Claimed(ClaimRef(ns.queue, key, Token(token)), message, attempt.toInt, Instant.ofEpochMilli(deadline))))
            case _                              => ZIO.fail(QueueError.MalformedReply(s"consume returned ${Lua.describe(values)}"))
        case other                                         => ZIO.fail(QueueError.MalformedReply(s"consume returned ${Lua.describe(other)}"))

  /**
   * Hand back whatever this connection was holding mid-claim — at most one key, since it makes one `BLMOVE`
   * at a time.
   *
   * @param ns the queue
   * @return noop; failures are ignored, because the sweep repairs what this missed
   */
  private def release(ns: Namespace): UIO[Unit] =
    ZIO.attemptBlocking(redis.lmove(ns.claiming(worker), ns.ready, LMoveArgs.Builder.rightLeft())).ignore.unit


object Claimer:

  /**
   * One claimer over its own connection.
   *
   * @param redis the connection it owns
   * @param scripts the loaded digests
   * @param worker its identity
   * @param leaseTtl the lease it writes
   * @return the claimer
   */
  def make(
    redis: RedisCommands[String, Array[Byte]],
    scripts: Scripts,
    worker: WorkerId,
    leaseTtl: Duration,
  ): UIO[Claimer] =
    Ref.make(Set.empty[QueueName]).map(Claimer(redis, scripts, worker, leaseTtl, _))
