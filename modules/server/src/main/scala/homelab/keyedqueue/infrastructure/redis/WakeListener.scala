package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.types.QueueName
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import io.lettuce.core.XReadArgs.StreamOffset
import io.lettuce.core.{ Limit, Range, XReadArgs }
import zio.*

import scala.jdk.CollectionConverters.*


/**
 * One blocking read across every wake stream in the deployment, routing each entry to the [[Readiness]]
 * that waits on it.
 *
 * '''One listener, many streams, `XREAD` being multi-stream.''' A single blocking read covers every wake
 * stream at once and returns the instant any of them has an entry — so the queue's bucket streams and the
 * lock's wake stream are served by one connection and one fiber. Which [[Readiness]] an entry wakes is
 * decided by the stream it came from (`routes`), so queue wakes and lock wakes stay logically separate
 * without a second listener or a second reserved connection.
 *
 * '''The set of streams is fixed, and that is the point.''' An `XREAD` names the streams it was issued
 * with, so the set is resolved once at startup — a queue or lock nobody has asked for yet is still heard
 * the instant something is appended for it, with no read re-issued.
 *
 * '''Why a stream and not pub/sub.''' A reader that reconnects resumes from the id it holds, so a blip
 * costs nothing; pub/sub would lose whatever arrived while it was away, and a lost wake is a waiter asleep
 * beside work it asked for.
 *
 * @param connection where the listening connection comes from
 * @param routes wake stream -> the readiness its entries wake
 * @param positions wake stream -> the last id delivered from it; the key set never changes
 * @param block how long one read waits before going round again
 */
final class WakeListener(
  connection: Connection,
  routes: Map[String, Readiness],
  positions: Ref[Map[String, String]],
  block: Duration,
):

  /**
   * Read the wake streams forever, routing each entry to its readiness.
   *
   * '''Supervised, because a dead listener is silent.''' A failed read is retried rather than killing the
   * fiber: a stopped listener leaves every waiter here parked beside work that is ready.
   *
   * '''A failure re-announces everything before retrying.''' Entries can be trimmed while a reader is away
   * and `XREAD` does not report stepping over any, so after a failure the safe assumption is that something
   * was missed — every readiness served is told to re-look. The backoff is short: it is the one interval a
   * waiter actually waits on.
   *
   * @return never completes
   */
  def run: UIO[Nothing] =
    read
      .flatMap(announce)
      .catchAll(_ => announceAll *> ZIO.sleep(WakeListener.retryBackoff))
      .forever *> ZIO.never

  /**
   * One `XREAD` across every wake stream, resuming from where each was left, paired with the readiness each
   * entry belongs to.
   *
   * @return the (readiness, name) wakes the entries carry, one per entry
   */
  private def read: IO[RedisFailure, Chunk[(Readiness, QueueName)]] =
    positions.get.flatMap: current =>
      val offsets = current.map((stream, id) => StreamOffset.from(stream, id)).toArray
      connection
        .listening(entries(offsets))
        .flatMap: delivered =>
          val woken = Chunk.fromIterable(delivered.flatMap: entry =>
            routes.get(entry.getStream).zip(WakeListener.nameOf(entry)))
          val ahead = delivered.map(entry => entry.getStream -> entry.getId).toMap
          positions.update(_.map((stream, id) => stream -> ahead.getOrElse(stream, id))).as(woken)

  /**
   * The raw read.
   *
   * @param offsets what to read, and from where
   * @return the entries that arrived, oldest first; aborts with `Unavailable` when the read fails
   */
  private def entries(
    offsets: Array[StreamOffset[String]]
  ): ZIO[Connection.Commands, RedisFailure, List[io.lettuce.core.StreamMessage[String, Array[Byte]]]] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.xread(XReadArgs.Builder.block(block.toMillis).count(WakeListener.count), offsets*))
        .mapBoth(LuaScript.failure, reply => Option(reply).map(_.asScala.toList).getOrElse(Nil))

  /**
   * Deliver each wake to its readiness, once.
   *
   * @param woken the (readiness, name) pairs this batch carried
   * @return noop
   */
  private def announce(woken: Chunk[(Readiness, QueueName)]): UIO[Unit] =
    ZIO.foreachDiscard(woken.distinct)((readiness, name) => readiness.ready(name))

  /**
   * Tell every readiness this listener serves to re-look — the failure path, where a wake may have been
   * missed and the safe move is to assume so.
   *
   * @return noop
   */
  private def announceAll: UIO[Unit] =
    ZIO.foreachDiscard(routes.values.toSet)(_.readyAll)


object WakeListener:

  /**
   * The most entries one read may return — a cap on a single reply, not a batch to fill. A high count
   * costs nothing in latency (a read returns as soon as one entry exists) and buys catch-up for a reader
   * that fell behind.
   */
  private val count: Long = 1000

  /** How long to wait after a failed read; short, because a retrying listener is a listener hearing nothing. */
  private val retryBackoff: Duration = 200.millis

  /**
   * The name an entry names — read from the `queue` field, which both queue and lock entries carry (a lock
   * release writes the lock name there), so one extractor serves every stream.
   *
   * @param entry one stream entry
   * @return the name it carries, or `None` when the entry has no `queue` field
   */
  private def nameOf(entry: io.lettuce.core.StreamMessage[String, Array[Byte]]): Option[QueueName] =
    Option(entry.getBody)
      .flatMap(body => Option(body.get("queue")))
      .map(bytes => QueueName(String(bytes, "UTF-8")))

  /**
   * A listener over the given wake streams, each positioned at its end, routing entries to their readiness.
   *
   * '''"From now" is resolved here, to a concrete id.''' Storing the `$` that means "the end of the stream"
   * would be a bug: it is re-evaluated by each read, so anything appended between two reads would be stepped
   * over. Resolving once means every read asks for "after the last entry I actually saw".
   *
   * @param connection where its connections come from
   * @param block how long one read waits before going round again
   * @param routes wake stream -> the readiness its entries wake
   * @return the listener; aborts with `Unavailable` when a stream's position cannot be read
   */
  def make(connection: Connection, block: Duration, routes: Map[String, Readiness]): IO[RedisFailure, WakeListener] =
    ZIO
      .foreach(Chunk.fromIterable(routes.keys))(stream => position(connection, stream).map(stream -> _))
      .flatMap(resolved => Ref.make(resolved.toMap))
      .map(WakeListener(connection, routes, _, block))

  /**
   * A listener over the queue's bucket wake streams, all waking one readiness — the queue's usual wiring.
   *
   * @param connection where its connections come from
   * @param readiness whose queues to announce
   * @param buckets how many wake streams the deployment has
   * @param block how long one read waits before going round again
   * @return the listener; aborts with `Unavailable` when a stream's position cannot be read
   */
  def make(connection: Connection, readiness: Readiness, buckets: Int, block: Duration): IO[RedisFailure, WakeListener] =
    make(connection, block, Namespace.wakeStreams(buckets).toChunk.map(_ -> readiness).toMap)

  /**
   * Where a wake stream is right now: the id of its last entry, or `0-0` when nothing has been appended.
   *
   * @param connection where to ask
   * @param stream the wake stream
   * @return the id to read after; aborts with `Unavailable` when the read fails
   */
  private def position(connection: Connection, stream: String): IO[RedisFailure, String] =
    connection.provide:
      Connection.use: redis =>
        ZIO
          .attemptBlocking(redis.xrevrange(stream, Range.unbounded[String](), Limit.create(0, 1)))
          .mapBoth(
            LuaScript.failure,
            reply => Option(reply).map(_.asScala.toList).getOrElse(Nil).headOption.map(_.getId).getOrElse("0-0"),
          )
