package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.types.QueueName
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import io.lettuce.core.XReadArgs.StreamOffset
import io.lettuce.core.{ Limit, Range, XReadArgs }
import zio.*

import scala.jdk.CollectionConverters.*


/**
 * The wake streams: one blocking read across every bucket in the deployment, raising the signal of each
 * queue that had a key become claimable.
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
 * '''Why a wake carries no work.''' It says "look again", and the consumer looks. Every instance reads
 * every entry, so several may look at once and one wins; the losers wait again. That is the price of never
 * losing a wake, and it is paid in round trips rather than in latency.
 *
 * @param connection where the listening connection comes from
 * @param waiters whose signals to raise
 * @param positions wake stream → the last id delivered from it; the key set never changes
 * @param block how long one read waits before going round again
 */
final class WakeListener(
  connection: Connection,
  waiters: Waiters,
  positions: Ref[Map[String, String]],
  block: Duration,
):

  /**
   * Read the wake streams forever, raising the signals of the queues named in what arrives.
   *
   * '''Supervised, because a dead listener is silent.''' A failed read — a connection blip, a topology
   * change — is retried rather than being allowed to kill the fiber. A listener that stopped would leave
   * every consumer on this instance waiting out its patience while work sat claimable, and nothing else
   * would report it.
   *
   * '''A failure raises everything before retrying.''' Entries can be trimmed while a reader is away, and
   * `XREAD` does not report having stepped over any, so after a failure the only safe assumption is that
   * something was announced and missed. The backoff is short and separate from [[block]] on purpose: the
   * block can be seconds because nothing waits on it, while this is the one place the interval is a
   * consumer's latency.
   *
   * @return never completes
   */
  def run: UIO[Nothing] =
    (read.flatMap(raise) *> ZIO.unit)
      .catchAll(_ => waiters.raiseAll *> ZIO.sleep(WakeListener.retryBackoff))
      .forever *> ZIO.never

  /**
   * One `XREAD` across every wake stream, resuming from where each was left.
   *
   * @return the queues named by the entries that arrived, one occurrence per entry
   */
  private def read: IO[QueueError, Chunk[QueueName]] =
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
  ): ZIO[Connection.Commands, QueueError, List[io.lettuce.core.StreamMessage[String, Array[Byte]]]] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.xread(XReadArgs.Builder.block(block.toMillis).count(WakeListener.count), offsets*))
        .mapBoth(LuaScript.failure, reply => Option(reply).map(_.asScala.toList).getOrElse(Nil))

  /**
   * Raise each named queue's signal, once.
   *
   * '''Deduplicated, which the signal makes correct.''' Raising reaches everyone holding a queue's signal,
   * so a thousand entries for one queue say exactly what one says. Under a registry that handed each
   * wake to one waiter this would have destroyed the cardinality the design ran on; here it is free, and it
   * is what lets a batch be large.
   *
   * @param woken the queues named by this batch, one occurrence per entry
   * @return noop
   */
  private def raise(woken: Chunk[QueueName]): UIO[Unit] = ZIO.foreachDiscard(woken.distinct)(waiters.raise)


object WakeListener:

  /**
   * The most entries one read may return.
   *
   * A cap on a single reply, not a batch to fill: a read returns as soon as one entry exists, so a high
   * count costs nothing in latency. What it buys is catch-up — a reader that fell behind drains in one
   * round trip rather than twenty — and after [[WakeListener.raise]] deduplicates, the work a batch causes
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
   * @param waiters whose signals to raise
   * @param buckets how many wake streams the deployment has
   * @param block how long one read waits before going round again
   * @return the listener; aborts with `StoreUnavailable` when a wake stream's position cannot be read
   */
  def make(connection: Connection, waiters: Waiters, buckets: Int, block: Duration): IO[QueueError, WakeListener] =
    ZIO
      .foreach(Namespace.wakeStreams(buckets).toChunk)(stream => position(connection, stream).map(stream -> _))
      .flatMap(resolved => Ref.make(resolved.toMap))
      .map(WakeListener(connection, waiters, _, block))

  /**
   * Where a wake stream is right now: the id of its last entry, or `0-0` when nothing has been appended.
   *
   * @param connection where to ask
   * @param stream the wake stream
   * @return the id to read after; aborts with `StoreUnavailable` when the read fails
   */
  private def position(connection: Connection, stream: String): IO[QueueError, String] =
    connection.provide:
      Connection.use: redis =>
        ZIO
          .attemptBlocking(redis.xrevrange(stream, Range.unbounded[String](), Limit.create(0, 1)))
          .mapBoth(
            LuaScript.failure,
            reply => Option(reply).map(_.asScala.toList).getOrElse(Nil).headOption.map(_.getId).getOrElse("0-0"),
          )
