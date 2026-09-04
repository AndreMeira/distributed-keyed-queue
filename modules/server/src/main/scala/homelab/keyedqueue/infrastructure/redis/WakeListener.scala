package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.error.QueueError
import homelab.keyedqueue.domain.types.QueueName
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import io.lettuce.core.XReadArgs.StreamOffset
import io.lettuce.core.{ Limit, Range, XReadArgs }
import zio.*

import scala.jdk.CollectionConverters.*


/**
 * The doorbell: one blocking read that hears every queue this instance waits on, and wakes one consumer
 * for each key that became claimable.
 *
 * '''Why a stream and not pub/sub.''' A reader that reconnects resumes from the id it holds, so a blip
 * costs nothing; pub/sub would lose whatever arrived while it was away, and a lost wake is a consumer
 * asleep beside work it asked for.
 *
 * '''Why one entry wakes one consumer.''' An entry means one key became claimable. Waking every waiter
 * would produce one claim and a crowd of wasted round trips — the cardinality is the point.
 *
 * '''Why the wake carries no work.''' It says "look again", and the consumer looks. Every instance reads
 * every entry, so several may look at once and one wins; the losers wait again. That is the price of never
 * losing a wake, and it is paid in round trips rather than in latency.
 *
 * @param connection where the listening connection comes from
 * @param waiters who to hand a wake to
 * @param watched queue → the last stream id delivered from it, resolved when the queue is first watched
 * @param block how long one read waits before going round again
 */
final class WakeListener(
  connection: Connection,
  waiters: Waiters,
  watched: Ref[Map[QueueName, String]],
  block: Duration,
):

  /**
   * Hear a queue's doorbell from now on.
   *
   * '''"From now" is resolved here, to a concrete id.''' Storing the `$` that means "the end of the
   * stream" would be a bug: it is evaluated by each read, so anything appended between two reads would be
   * stepped over and never delivered. Resolving once means every read asks for "after the last entry I
   * actually saw".
   *
   * Called before a consumer's first claim attempt, not after it: a queue that is not being listened to
   * when the attempt finds nothing would go unheard until the next caller arrives.
   *
   * @param queue the queue to hear
   * @return noop; aborts with `QueueError` when the stream's position cannot be read
   */
  def watch(queue: QueueName): IO[QueueError, Unit] =
    watched.get.flatMap: current =>
      if current.contains(queue) then ZIO.unit
      else position(queue).flatMap(id => watched.update(_.updated(queue, id)))

  /**
   * Wait for this queue's doorbell, for at most `patience`.
   *
   * Does not watch the queue: a caller watches before its first claim attempt, because a queue nobody is
   * listening to would go unheard exactly when the caller starts waiting on it. Watching here as well
   * would be a second place to look for that ordering, and only one of them can be the reason.
   *
   * @param queue what to wait for work on
   * @param patience the longest to wait
   * @return whether a wake arrived
   */
  def await(queue: QueueName, patience: Duration): UIO[Boolean] = waiters.waitFor(queue, patience)

  /**
   * Read the doorbells forever, waking one consumer per entry.
   *
   * '''Supervised, because a dead listener is silent.''' A failed read — a connection blip, a topology
   * change — is retried after a pause rather than being allowed to kill the fiber. A listener that stopped
   * would leave every consumer on this instance waiting out its patience while work sat claimable, and
   * nothing else would report it.
   *
   * @return never completes
   */
  def run: UIO[Nothing] =
    (read.flatMap(wake) *> ZIO.unit).catchAll(_ => ZIO.sleep(block)).forever *> ZIO.never

  /**
   * One `XREAD` across every watched queue, resuming from where each was left.
   *
   * Watching nothing is a sleep rather than a read: `XREAD` needs at least one stream, and an instance
   * serving no queue yet has nobody to wake.
   *
   * @return the queues that had entries, one occurrence per entry
   */
  private def read: IO[QueueError, Chunk[QueueName]] =
    watched.get.flatMap: current =>
      if current.isEmpty then ZIO.sleep(block).as(Chunk.empty)
      else
        val watching = current.toList.map((queue, id) => (queue, Namespace(queue).wake, id))
        val offsets  = watching.map((_, stream, id) => StreamOffset.from(stream, id)).toArray
        val queueOf  = watching.map((queue, stream, _) => stream -> queue).toMap
        connection
          .listening(entries(offsets))
          .flatMap: delivered =>
            val rung  = Chunk.fromIterable(delivered.flatMap((stream, _) => queueOf.get(stream)))
            val ahead = delivered.flatMap((stream, id) => queueOf.get(stream).map(_ -> id)).toMap
            watched.update(_.map((queue, id) => queue -> ahead.getOrElse(queue, id))).as(rung)

  /**
   * The raw read, as stream/id pairs.
   *
   * @param offsets what to read, and from where
   * @return the entries that arrived, oldest first; aborts with `StoreUnavailable` when the read fails
   */
  private def entries(offsets: Array[StreamOffset[String]]): ZIO[Connection.Commands, QueueError, List[(String, String)]] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.xread(XReadArgs.Builder.block(block.toMillis).count(64), offsets*))
        .mapBoth(
          LuaScript.failure,
          reply => Option(reply).map(_.asScala.toList).getOrElse(Nil).map(entry => entry.getStream -> entry.getId),
        )

  /**
   * Wake one consumer per entry.
   *
   * @param queues the queues that had entries, one occurrence per entry
   * @return noop
   */
  private def wake(queues: Chunk[QueueName]): UIO[Unit] = ZIO.foreachDiscard(queues)(waiters.wake)

  /**
   * Where a queue's doorbell is right now: the id of its last entry, or `0-0` when it has never rung.
   *
   * @param queue the queue
   * @return the id to read after; aborts with `StoreUnavailable` when the read fails
   */
  private def position(queue: QueueName): IO[QueueError, String] =
    connection.provide:
      Connection.use: redis =>
        ZIO
          .attemptBlocking(redis.xrevrange(Namespace(queue).wake, Range.unbounded[String](), Limit.create(0, 1)))
          .mapBoth(
            LuaScript.failure,
            reply => Option(reply).map(_.asScala.toList).getOrElse(Nil).headOption.map(_.getId).getOrElse("0-0"),
          )


object WakeListener:

  /**
   * A listener, hearing nothing yet.
   *
   * @param connection where its connections come from
   * @param waiters who to hand wakes to
   * @param block how long one read waits before going round again — also the longest a newly watched queue
   *              can go unheard, since the read in flight names the streams it was issued with
   * @return the listener
   */
  def make(connection: Connection, waiters: Waiters, block: Duration): UIO[WakeListener] =
    Ref.make(Map.empty[QueueName, String]).map(WakeListener(connection, waiters, _, block))
