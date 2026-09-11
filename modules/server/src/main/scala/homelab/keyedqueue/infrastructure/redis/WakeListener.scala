package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.types.QueueName
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import io.lettuce.core.XReadArgs.StreamOffset
import io.lettuce.core.{ Limit, Range, XReadArgs }
import zio.*

import scala.jdk.CollectionConverters.*


/**
 * A blocking read per slot group of wake streams, routing each entry to the [[Waker]] that waits on it.
 *
 * '''One fiber per reader, because the cluster bounds what one read may name.''' `XREAD` is multi-stream,
 * but Redis Cluster rejects a multi-key read whose keys span slots — and the partition tags exist precisely to
 * spread slots. [[Connection]] therefore opens a reader per group of streams that one command may name, and
 * this runs a fiber on each; on a single server there is one group, so one connection and one fiber — the
 * original design. Which [[Waker]] an entry wakes is decided by the stream it came from (`routes`), so
 * queue wakes and lock wakes stay logically separate without separate listeners.
 *
 * '''The set of streams is fixed, and that is the point.''' An `XREAD` names the streams it was issued
 * with, so the set is resolved once at startup — a queue or lock nobody has asked for yet is still heard
 * the instant something is appended for it, with no read re-issued.
 *
 * '''Why a stream and not pub/sub.''' A reader that reconnects resumes from the id it holds, so a blip
 * costs nothing; pub/sub would lose whatever arrived while it was away, and a lost wake is a waiter asleep
 * beside work it asked for.
 *
 * @param readers the connections to block on, each with the streams one read may name
 * @param routes wake stream -> the readiness its entries wake
 * @param positions wake stream -> the last id delivered from it; the key set never changes
 * @param block how long one read waits before going round again
 */
final class WakeListener(
  readers: Chunk[Connection.Reader],
  routes: Map[String, Waker],
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
    ZIO.foreachParDiscard(readers)(loop) *> ZIO.never

  /**
   * One group's read loop, forever.
   *
   * @param reader the connection to block on, and the streams one read may name
   * @return never completes
   */
  private def loop(reader: Connection.Reader): UIO[Unit] =
    val Connection.Reader(commands, streams) = reader
    read(commands, streams)
      .flatMap(announce)
      .catchAll: error =>
        // Announce-all is the safe recovery for a missed read — but a reader that lands here every round
        // has degraded into interval polling, which the log line is here to make visible.
        ZIO.logWarning(s"wake read failed, announcing the group as recovery: ${error.message}")
          *> announceGroup(streams) *> ZIO.sleep(WakeListener.retryBackoff)
      .forever

  /**
   * One `XREAD` across this group's streams, resuming from where each was left, paired with the readiness
   * each entry belongs to.
   *
   * @param commands the group's connection
   * @param streams the streams it reads
   * @return the (readiness, name) wakes the entries carry, one per entry
   */
  private def read(
    commands: Connection.Commands,
    streams: Chunk[String],
  ): IO[RedisFailure, Chunk[(Waker, QueueName)]] =
    positions.get.flatMap: current =>
      val offsets = streams.map(stream => StreamOffset.from(stream, current(stream))).toArray
      entries(commands, offsets).flatMap: delivered =>
        val woken = Chunk.fromIterable(delivered.flatMap: entry =>
          routes.get(entry.getStream).zip(WakeListener.nameOf(entry)))
        val ahead = delivered.map(entry => entry.getStream -> entry.getId).toMap
        positions.update(_.map((stream, id) => stream -> ahead.getOrElse(stream, id))).as(woken)

  /**
   * The raw read.
   *
   * @param commands the connection to block on
   * @param offsets what to read, and from where
   * @return the entries that arrived, oldest first; aborts with `Unavailable` when the read fails
   */
  private def entries(
    commands: Connection.Commands,
    offsets: Array[StreamOffset[String]],
  ): IO[RedisFailure, List[io.lettuce.core.StreamMessage[String, Array[Byte]]]] =
    ZIO
      .attemptBlocking(commands.xread(XReadArgs.Builder.block(block.toMillis).count(WakeListener.count), offsets*))
      .mapBoth(LuaScript.failure, reply => Option(reply).map(_.asScala.toList).getOrElse(Nil))

  /**
   * Deliver each wake to its readiness, once.
   *
   * @param woken the (readiness, name) pairs this batch carried
   * @return noop
   */
  private def announce(woken: Chunk[(Waker, QueueName)]): UIO[Unit] =
    ZIO.foreachDiscard(woken.distinct)((waker, name) => waker.ready(name))

  /**
   * Tell this group's wakers to re-look — the failure path, where a wake may have been missed and the safe
   * move is to assume so.
   *
   * @param streams the group's streams
   * @return noop
   */
  private def announceGroup(streams: Chunk[String]): UIO[Unit] =
    ZIO.foreachDiscard(streams.flatMap(routes.get).toSet)(_.readyAll)


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
   * @param connection whose readers this listens on, and whose shared connection resolves the positions
   * @param block how long one read waits before going round again
   * @param routes wake stream -> the waker its entries wake
   * @return the listener; aborts with `Unavailable` when a stream's position cannot be read
   */
  def make(connection: Connection, block: Duration, routes: Map[String, Waker]): IO[RedisFailure, WakeListener] =
    positioned(connection).map(WakeListener(connection.readers, routes, _, block))

  /**
   * Where each stream this listener will read stands right now.
   *
   * Taken from the readers rather than from `routes`, so what is positioned is exactly what will be read —
   * there is no third opinion about which streams exist.
   *
   * @param connection whose readers name the streams, and whose shared connection answers
   * @return the positions, ready to read from; aborts with `Unavailable` when one cannot be read
   */
  private def positioned(connection: Connection): IO[RedisFailure, Ref[Map[String, String]]] =
    ZIO
      .foreach(connection.readers.flatMap(_.streams))(stream => position(connection, stream).map(stream -> _))
      .flatMap(resolved => Ref.make(resolved.toMap))

  /**
   * A listener over the queue's partition wake streams, all waking one readiness — the queue's usual wiring.
   *
   * @param connection where its connections come from
   * @param readiness whose queues to announce
   * @param block how long one read waits before going round again
   * @return the listener; aborts with `Unavailable` when a stream's position cannot be read
   */
  def make(connection: Connection, readiness: Readiness, block: Duration): ZIO[Scope, RedisFailure, WakeListener] =
    make(connection, block, Namespace.wakeStreams.toChunk.map(stream => stream -> (readiness: Waker)).toMap)

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
