package homelab.keyedqueue.infrastructure.redis


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.types.QueueName
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import io.lettuce.core.XReadArgs.StreamOffset
import io.lettuce.core.{ Limit, Range, XReadArgs }
import zio.*

import scala.jdk.CollectionConverters.*


/**
 * The wake streams: one blocking read across every bucket in the deployment, announcing each queue that had
 * a key become claimable.
 *
 * '''The set of streams is fixed, and that is the point.''' An `XREAD` names the streams it was issued
 * with, so a per-queue wake stream meant the set grew as queues were served, and a queue asked for while
 * a read was in flight went unheard until that read returned — `block` on the latency path of every
 * queue's first consumer. Buckets are known before any queue is served, so every wake stream is in every
 * read from the first one: a queue nobody has ever asked for is heard the instant something is appended
 * for it.
 *
 * '''Why a stream and not pub/sub.''' A reader that reconnects resumes from the id it holds, so a blip
 * costs nothing; pub/sub would lose whatever arrived while it was away, and a lost wake is a consumer
 * asleep beside work it asked for.
 *
 * @param connection where the listening connection comes from
 * @param readiness whose queues to announce
 * @param positions wake stream → the last id delivered from it; the key set never changes
 * @param block how long one read waits before going round again
 */
final class WakeListener(
  connection: Connection,
  readiness: Readiness,
  positions: Ref[Map[String, String]],
  block: Duration,
):

  /**
   * Read the wake streams forever, announcing the queues named in what arrives.
   *
   * '''Supervised, because a dead listener is silent.''' A failed read is retried rather than killing the
   * fiber: a stopped listener leaves every consumer here waiting out its patience beside claimable work.
   *
   * '''A failure announces everything before retrying.''' Entries can be trimmed while a reader is away and
   * `XREAD` does not report stepping over any, so after a failure the safe assumption is that something was
   * missed. The backoff is short: it is the one interval that is a consumer's latency.
   *
   * @return never completes
   */
  def run: UIO[Nothing] =
    (read.flatMap(announce) *> ZIO.unit)
      .catchAll(_ => readiness.readyAll *> ZIO.sleep(WakeListener.retryBackoff))
      .forever *> ZIO.never

  /**
   * One `XREAD` across every wake stream, resuming from where each was left.
   *
   * @return the queues named by the entries that arrived, one occurrence per entry
   */
  private def read: IO[RedisFailure, Chunk[QueueName]] =
    positions.get.flatMap: current =>
      val offsets = current.map((stream, id) => StreamOffset.from(stream, id)).toArray
      connection
        .listening(entries(offsets))
        .flatMap: delivered =>
          val woken = Chunk.fromIterable(delivered.flatMap(WakeListener.queueOf))
          val ahead = delivered.map(entry => entry.getStream -> entry.getId).toMap
          positions.update(_.map((stream, id) => stream -> ahead.getOrElse(stream, id))).as(woken)

  /**
   * The raw read.
   *
   * @param offsets what to read, and from where
   * @return the entries that arrived, oldest first; aborts with `StoreUnavailable` when the read fails
   */
  private def entries(
    offsets: Array[StreamOffset[String]]
  ): ZIO[Connection.Commands, RedisFailure, List[io.lettuce.core.StreamMessage[String, Array[Byte]]]] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.xread(XReadArgs.Builder.block(block.toMillis).count(WakeListener.count), offsets*))
        .mapBoth(LuaScript.failure, reply => Option(reply).map(_.asScala.toList).getOrElse(Nil))

  /**
   * Announce each named queue, once.
   *
   * @param woken the queues named by this batch, one occurrence per entry
   * @return noop
   */
  private def announce(woken: Chunk[QueueName]): UIO[Unit] = ZIO.foreachDiscard(woken.distinct)(readiness.ready)


object WakeListener:

  /**
   * The most entries one read may return.
   *
   * A cap on a single reply, not a batch to fill: a read returns as soon as one entry exists, so a high
   * count costs nothing in latency. What it buys is catch-up — a reader that fell behind drains in one
   * round trip rather than twenty — and after [[WakeListener.announce]] deduplicates, the work a batch causes
   * is proportional to the queues in it rather than to its size.
   */
  private val count: Long = 1000

  /**
   * How long to wait after a failed read.
   *
   * Separate from the block, and much shorter: this is the one interval a consumer actually waits on, since
   * a listener that is retrying is a listener hearing nothing.
   */
  private val retryBackoff: Duration = 200.millis

  /**
   * Which queue an entry concerns.
   *
   * The stream no longer says — a wake stream is shared by every queue in its bucket — so the queue travels
   * in the entry, written by the script that appended it.
   *
   * @param entry one stream entry
   * @return the queue it names, or `None` when the entry has no `queue` field
   */
  private def queueOf(entry: io.lettuce.core.StreamMessage[String, Array[Byte]]): Option[QueueName] =
    Option(entry.getBody)
      .flatMap(body => Option(body.get("queue")))
      .map(bytes => QueueName(String(bytes, "UTF-8")))

  /**
   * A listener over every wake stream in the deployment, each positioned at its end.
   *
   * '''"From now" is resolved here, to a concrete id.''' Storing the `$` that means "the end of the
   * stream" would be a bug: it is evaluated by each read, so anything appended between two reads would be
   * stepped over and never delivered. Resolving once means every read asks for "after the last entry I
   * actually saw".
   *
   * From now rather than from the beginning, because a wake that arrived before this instance existed
   * announced work that is either still claimable — and found by the next claim — or already taken.
   *
   * @param connection where its connections come from
   * @param readiness whose queues to announce
   * @param buckets how many wake streams the deployment has
   * @param block how long one read waits before going round again
   * @return the listener; aborts with `StoreUnavailable` when a wake stream's position cannot be read
   */
  def make(connection: Connection, readiness: Readiness, buckets: Int, block: Duration): IO[RedisFailure, WakeListener] =
    ZIO
      .foreach(Namespace.wakeStreams(buckets).toChunk)(stream => position(connection, stream).map(stream -> _))
      .flatMap(resolved => Ref.make(resolved.toMap))
      .map(WakeListener(connection, readiness, _, block))

  /**
   * Where a wake stream is right now: the id of its last entry, or `0-0` when nothing has been appended.
   *
   * @param connection where to ask
   * @param stream the wake stream
   * @return the id to read after; aborts with `StoreUnavailable` when the read fails
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
