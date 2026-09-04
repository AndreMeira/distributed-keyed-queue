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
 * '''Why a silent turn rings anyway.''' Carrying no work is also what makes the backstop affordable: a
 * read that heard nothing wakes whoever is parked, so a wake lost with the consumer that took it costs a
 * wait of `block` rather than lasting until the queue sees traffic again. See the note on [[wake]].
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
   * Wake one consumer per entry, or — on a quiet turn — one per queue somebody is parked on.
   *
   * '''The backstop.''' A wake is taken by a consumer and carried back as a value, and a fiber interrupted
   * between those two moments drops it where no finalizer can see: the effect succeeds, the fiber fails,
   * and the key stays claimable with this instance's consumers asleep beside it. The same gap sits between
   * [[Waiters.waitFor]] answering and the claim it answers for. Nothing inside a dead fiber can repair
   * that, so a turn that heard nothing rings whoever is waiting instead — one look per parked queue, and
   * only while the doorbell is silent, which is to say only while the queue is idle anyway.
   *
   * @param rung the queues that had entries, one occurrence per entry
   * @return noop
   */
  private def wake(rung: Chunk[QueueName]): UIO[Unit] =
    if rung.nonEmpty then ZIO.foreachDiscard(rung)(waiters.wake)
    else waiters.parked.flatMap(ZIO.foreachDiscard(_)(waiters.wake))

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
