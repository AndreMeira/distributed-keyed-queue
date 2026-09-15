package homelab.keyedqueue.infrastructure.redis


import homelab.common.messaging.Consumer
import homelab.keyedqueue.infrastructure.redis.keys.{ KeyLayout, RedisKey }
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import io.lettuce.core.XReadArgs.StreamOffset
import io.lettuce.core.{ Limit, Range, StreamMessage, XReadArgs }
import zio.*

import scala.jdk.CollectionConverters.*


/**
 * The wake streams, as a [[Consumer.Batched]] of [[Wake]]s: everything about Redis that the wake path needs,
 * behind one `consume`.
 *
 * '''One reader per partition, one consumer for all of them.''' A blocking `XREAD` holds its connection for
 * the whole wait and may not span slots, so the reads are genuinely separate; they feed a queue that
 * `consume` drains, which is what lets a caller see one intake however the streams are spread.
 *
 * '''A batch is what had accumulated, not what one read returned.''' Draining coalesces a burst into a
 * single delivery, and repeated names within it collapse: a wake says a name may have something, not how
 * many times.
 *
 * A read that fails does not reach the caller — the reader logs it, backs off, and carries on from the id it
 * already holds.
 *
 * @param connection the blocking connections the streams are read on
 * @param layout which partitions there are, and the stream each one announces on
 * @param positions wake stream -> the last entry id read from it; the key set never changes
 * @param wakes what the readers fill and `consume` drains
 * @param block how long one read waits before going round again
 */
final class WakeConsumer(
  connection: Connection,
  layout: KeyLayout,
  positions: Ref[Map[RedisKey, WakeConsumer.EntryId]],
  wakes: Queue[Wake],
  block: Duration,
) extends Consumer.Batched[RedisFailure, Wake]:

  /**
   * Take everything that has arrived, waiting for the first, and hand it over as one batch.
   *
   * @param logic what to do with the batch
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the batch is handled
   */
  override def consume[E2 >: RedisFailure](logic: List[Wake] => IO[E2, Unit]): IO[E2, Unit] =
    wakes.takeBetween(1, WakeConsumer.drain).flatMap(batch => logic(batch.distinct.toList))

  /**
   * Read every partition's stream, forever, into the queue.
   *
   * @return never completes
   */
  def start: UIO[Nothing] =
    ZIO.foreachParDiscard(layout.partitionIds) { partition =>
      read(partition).flatMap(wakes.offerAll).catchAll(recovered).forever
    } *> ZIO.never

  /**
   * Check that every partition has a connection to read on, before any reader is forked.
   *
   * A partition without one is this code wired wrong, and a reader cannot report it: its loop logs every
   * failure and retries, so the mistake would show up as a stream nobody reads. Refusing here makes it a
   * startup failure instead.
   *
   * @return noop; aborts with `PartitionConnectionMissing` when a partition has no connection
   */
  def reachable: IO[RedisFailure, Unit] =
    ZIO.foreachDiscard(layout.partitionIds): partition =>
      connection.provideBlocking(partition)(ZIO.unit)

  /**
   * Record where each stream this will read stands right now.
   *
   * Taken from the same stream set the reads are issued with, so what is positioned is exactly what will be
   * read.
   *
   * @return noop; aborts with `Unavailable` when a position cannot be read
   */
  def positioned: IO[RedisFailure, Unit] =
    connection.provide:
      ZIO
        .foreach(layout.wakeStreamKeys.toChunk)(position)
        .flatMap(resolved => positions.set(resolved.toMap))

  /**
   * What a failed read leaves behind: a line in the log, and a pause before trying again.
   *
   * '''No gap is reported.''' This stream replays — the next read resumes from the id this one holds, so a
   * failure delays entries rather than losing them. What it cannot see is a trim while it was away, which
   * costs latency (a waiter sleeps until its recheck or its patience) and never work. A reader that lands
   * here every round has degraded into interval polling, which the log line is here to make visible.
   *
   * @param error why the read failed
   * @return noop
   */
  private def recovered(error: RedisFailure): UIO[Unit] =
    ZIO.logWarning(s"wake read failed, retrying: ${error.message}")
      *> ZIO.sleep(WakeConsumer.retryBackoff)

  /**
   * One `XREAD` on this partition's stream, resuming from where it was left.
   *
   * @param partition the partition whose wake stream to read
   * @return what the entries announce; one naming no known kind is dropped; aborts with `Unavailable` when
   *         the read fails
   */
  private def read(partition: KeyLayout.Partition): IO[RedisFailure, Chunk[Wake]] =
    for
      stream     = layout.wakeStream(partition)
      current   <- positions.get
      from       = current.getOrElse(stream, WakeConsumer.EntryId.beginning)
      delivered <- entries(partition, StreamOffset.from[String](stream, from))
      _         <- advanced(delivered)
    yield Chunk.fromIterable(delivered.flatMap(WakeConsumer.wakeOf))

  /**
   * Move each stream's position to the last entry it delivered.
   *
   * @param delivered what the read returned, oldest first
   * @return noop
   */
  private def advanced(delivered: List[StreamMessage[String, Array[Byte]]]): UIO[Unit] =
    val ahead = delivered.map(entry => RedisKey(entry.getStream) -> WakeConsumer.EntryId(entry.getId)).toMap
    positions.update(_.map((stream, id) => stream -> ahead.getOrElse(stream, id)))

  /**
   * The raw read, on the connection this partition has to itself.
   *
   * @param partition whose connection to block on
   * @param offset what to read, and from where
   * @return the entries that arrived, oldest first; aborts with `Unavailable` when the read fails, and with
   *         `PartitionConnectionMissing` when this partition has no connection to read on
   */
  private def entries(
    partition: KeyLayout.Partition,
    offset: StreamOffset[String],
  ): IO[RedisFailure, List[StreamMessage[String, Array[Byte]]]] =
    connection.provideBlocking(partition):
      Connection.use: commands =>
        ZIO
          .attemptBlocking:
            commands.xread(XReadArgs.Builder.block(block.toMillis).count(WakeConsumer.count), offset)
          .mapBoth(LuaScript.failure, reply => Option(reply).map(_.asScala.toList).getOrElse(Nil))

  /**
   * Where a wake stream is right now: the id of its last entry, or the beginning when nothing has been
   * appended.
   *
   * Asked on the shared connection: this answers at once, and the per-partition ones are for commands that
   * block.
   *
   * @param stream the wake stream
   * @return the stream, with the id to read after it; aborts with `Unavailable` when the read fails
   */
  private def position(
    stream: RedisKey
  ): ZIO[Connection.Commands, RedisFailure, (RedisKey, WakeConsumer.EntryId)] =
    Connection.use: commands =>
      ZIO
        .attemptBlocking(commands.xrevrange(stream, Range.unbounded[String](), Limit.create(0, 1)))
        .mapBoth(LuaScript.failure, reply => stream -> WakeConsumer.EntryId(Option(reply)))


object WakeConsumer:

  /**
   * The most entries one read may return — a cap on a single reply, not a batch to fill. A high count costs
   * nothing in latency (a read returns as soon as one entry exists) and buys catch-up for a reader that
   * fell behind.
   */
  private val count: Long = 1000

  /** The most wakes one `consume` drains at once; a burst larger than this arrives as several batches. */
  private val drain: Int = 1000

  /** How long to wait after a failed read; short, because a retrying reader is a reader hearing nothing. */
  private val retryBackoff: Duration = 200.millis

  /**
   * Where a reader has got to in a stream: the id of the last entry it was handed.
   *
   * A stream's name and a position in it are both strings, and they travel together. Naming this one is
   * what stops the pair being passed the wrong way round, which would compile and resume from a position
   * that means nothing.
   */
  type EntryId = EntryId.Type

  object EntryId:

    opaque type Type <: String = String

    /** Where a stream nothing has been appended to stands: read after this and the first entry is next. */
    val beginning: Type = "0-0"

    /**
     * An id, trusted.
     *
     * @param value the id as Redis reported it
     * @return the id
     */
    def apply(value: String): Type = value

    /**
     * Where a stream stands, from what `XREVRANGE … COUNT 1` answered: the id of its newest entry, or
     * [[beginning]] when the stream is empty or has never been written.
     *
     * @param reply what Redis answered, absent when it answered nothing at all
     * @return the id to read after
     */
    def apply(reply: Option[java.util.List[StreamMessage[String, Array[Byte]]]]): Type =
      reply.flatMap(_.asScala.toList.headOption).map(_.getId).getOrElse(beginning)

  /**
   * A consumer over every partition's wake stream, each positioned at its end, already reading.
   *
   * '''"From now" is resolved here, to a concrete id.''' Storing the `$` that means "the end of the stream"
   * would be a bug: it is re-evaluated by each read, so anything appended between two reads would be
   * stepped over.
   *
   * @param connection the blocking connections to read on, and the shared one that resolves the positions
   * @param layout which partitions there are, and the stream each one announces on
   * @param block how long one read waits before going round again
   * @return the consumer, its readers forked into the scope; aborts with `PartitionConnectionMissing` when
   *         a partition has no connection, and with `Unavailable` when a stream's position cannot be read
   */
  def make(connection: Connection, layout: KeyLayout, block: Duration): ZIO[Scope, RedisFailure, WakeConsumer] =
    for
      wakes     <- Queue.unbounded[Wake]
      positions <- Ref.make(Map.empty[RedisKey, EntryId])
    yield WakeConsumer(connection, layout, positions, wakes, block)

  /**
   * What an entry announces: the kind of thing, and its name.
   *
   * Both fields are required, so an entry missing either — or naming a kind this code does not have — is
   * not something this can deliver, and is reported as nothing to deliver.
   *
   * @param entry one stream entry
   * @return what it announces, absent when either field cannot be read
   */
  private def wakeOf(entry: StreamMessage[String, Array[Byte]]): Option[Wake] =
    for
      body <- Option(entry.getBody)
      kind <- Option(body.get("kind")).map(field)
      name <- Option(body.get("name")).map(field)
      wake <- Wake.read(kind, name)
    yield wake

  /**
   * One entry field, as it was written.
   *
   * @param bytes the field's value
   * @return its text
   */
  private def field(bytes: Array[Byte]): String = String(bytes, "UTF-8")
