package homelab.keyedqueue.infrastructure.redis


import homelab.common.messaging.Consumer
import homelab.keyedqueue.infrastructure.redis.keys.{ KeyLayout, RedisKey }
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import io.lettuce.core.XReadArgs.StreamOffset
import io.lettuce.core.{ Limit, Range, StreamMessage, XReadArgs }
import zio.*

import scala.jdk.CollectionConverters.*


/**
 * The wake streams, as a [[Consumer]] of [[Wake]]s: everything about Redis that the wake path needs, behind
 * one `consume`.
 *
 * One fiber per partition, each blocked on that partition's stream and feeding a queue that `consume` takes
 * from — so a caller sees one source however the streams are spread. A read that fails does not abort the
 * caller: the reader offers a [[Wake.Gap]], backs off and carries on, because losing a position is news its
 * consumer can act on rather than a failure to hand upwards.
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
) extends Consumer[RedisFailure, Wake]:

  /**
   * Take the next wake and run `logic` on it, waiting for one if none has arrived.
   *
   * @param logic what to do with the wake
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the wake is handled
   */
  override def consume[E2 >: RedisFailure](logic: Wake => IO[E2, Unit]): IO[E2, Unit] =
    wakes.take.flatMap(logic)

  /**
   * Read every partition's stream, forever, into the queue.
   *
   * @return never completes
   */
  private[redis] def listen: UIO[Nothing] =
    ZIO.foreachParDiscard(layout.partitionIds)(loop) *> ZIO.never

  /**
   * One partition's read loop, forever.
   *
   * @param partition whose wake stream to read, on the connection opened for it
   * @return never completes
   */
  private def loop(partition: KeyLayout.Partition): UIO[Unit] =
    poll(partition).flatMap(wakes.offerAll).catchAll(recovered).forever

  /**
   * What a failed read leaves behind: a gap for the consumer, and a pause before trying again.
   *
   * A reader that lands here every round has degraded into interval polling, which the log line is here to
   * make visible.
   *
   * @param error why the read failed
   * @return noop
   */
  private def recovered(error: RedisFailure): UIO[Unit] =
    ZIO.logWarning(s"wake read failed, reporting a gap: ${error.message}")
      *> wakes.offer(Wake.Gap)
      *> ZIO.sleep(WakeConsumer.retryBackoff)

  /**
   * One `XREAD` on this partition's stream, resuming from where it was left.
   *
   * '''Deduplicated across the batch''', which only a reader can do: a wake says a name may have something,
   * not how many times, so a hundred entries naming one queue are one wake. Past here the entries are taken
   * one at a time and the batch no longer exists.
   *
   * @param partition the partition whose wake stream to read
   * @return what the entries announce, each name once; one naming no known kind is dropped
   */
  private def poll(partition: KeyLayout.Partition): IO[RedisFailure, Chunk[Wake]] =
    positions.get.flatMap: current =>
      // `from[String]` pins what Lettuce is handed: a StreamOffset[RedisKey] would not be a
      // StreamOffset[String], and the builder is invariant.
      val stream = layout.wakeStream(partition)
      val from   = current.getOrElse(stream, WakeConsumer.EntryId.beginning)
      entries(partition, StreamOffset.from[String](stream, from)).flatMap: delivered =>
        val woken = Chunk.fromIterable(delivered.flatMap(WakeConsumer.wakeOf)).distinct
        val ahead = delivered.map(entry => RedisKey(entry.getStream) -> WakeConsumer.EntryId(entry.getId)).toMap
        positions.update(_.map((stream, id) => stream -> ahead.getOrElse(stream, id))).as(woken)

  /**
   * Check that every partition has a connection to read on, before any reader is forked.
   *
   * A partition without one is this code wired wrong, and a reader cannot report it: its loop turns every
   * failure into a gap and retries, so the mistake would show up as every waiter being woken several times
   * a second, forever. Refusing here makes it a startup failure instead.
   *
   * @return noop; aborts with `PartitionConnectionMissing` when a partition has no connection
   */
  private[redis] def reachable: IO[RedisFailure, Unit] =
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
  private[redis] def positioned: IO[RedisFailure, Unit] =
    connection.provide:
      ZIO
        .foreach(layout.wakeStreamKeys.toChunk)(position)
        .flatMap(resolved => positions.set(resolved.toMap))

  /**
   * Where a wake stream is right now: the id of its last entry, or `0-0` when nothing has been appended.
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

  /**
   * The raw read, on the connection this partition has to itself.
   *
   * @param partition whose connection to block on
   * @param offset what to read, and from where
   * @return the entries that arrived, oldest first; aborts with `Unavailable` when the read fails
   */
  private def entries(
    partition: KeyLayout.Partition,
    offset: StreamOffset[String],
  ): IO[RedisFailure, List[StreamMessage[String, Array[Byte]]]] =
    connection.provideBlocking(partition):
      Connection.use: commands =>
        ZIO
          .attemptBlocking(commands.xread(XReadArgs.Builder.block(block.toMillis).count(WakeConsumer.count), offset))
          .mapBoth(LuaScript.failure, reply => Option(reply).map(_.asScala.toList).getOrElse(Nil))


object WakeConsumer:

  /**
   * The most entries one read may return — a cap on a single reply, not a batch to fill. A high count costs
   * nothing in latency (a read returns as soon as one entry exists) and buys catch-up for a reader that
   * fell behind.
   */
  private val count: Long = 1000

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
   *         a partition has no connection to read on, and with `Unavailable` when a stream's position
   *         cannot be read
   */
  def make(connection: Connection, layout: KeyLayout, block: Duration): ZIO[Scope, RedisFailure, WakeConsumer] =
    for
      wakes     <- Queue.unbounded[Wake]
      positions <- Ref.make(Map.empty[RedisKey, EntryId])
      consumer   = WakeConsumer(connection, layout, positions, wakes, block)
      _         <- consumer.reachable
      _         <- consumer.positioned
      _         <- consumer.listen.forkScoped
    yield consumer

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
