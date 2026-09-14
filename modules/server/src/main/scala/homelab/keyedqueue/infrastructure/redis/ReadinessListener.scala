package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.{ LockName, QueueName }
import homelab.keyedqueue.infrastructure.redis.keys.{ KeyLayout, RedisKey }
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import io.lettuce.core.XReadArgs.StreamOffset
import io.lettuce.core.{ Limit, Range, StreamMessage, XReadArgs }
import zio.*

import scala.jdk.CollectionConverters.*


/**
 * A blocking read per partition, delivering each entry to the readiness that waits on it.
 *
 * One fiber per partition, each blocked on that partition's wake stream. Which readiness an entry reaches
 * is decided by the kind the entry carries rather than by the stream it arrived on, so queue wakes and lock
 * wakes stay separate while sharing a stream. The set of streams is resolved at startup, so a queue or lock
 * nobody has asked for yet is heard the instant something is appended for it.
 *
 * @param connection the blocking connections this reads on
 * @param layout which partitions there are, and the stream each one announces on
 * @param queueReady where queue wakes go
 * @param lockReady where lock wakes go
 * @param positions wake stream -> the last entry id delivered from it; the key set never changes
 * @param block how long one read waits before going round again
 */
final class ReadinessListener(
  connection: Connection,
  layout: KeyLayout,
  queueReady: QueueReadiness,
  lockReady: LockReadiness,
  positions: Ref[Map[RedisKey, ReadinessListener.EntryId]],
  block: Duration,
):

  /**
   * Read the wake streams forever, routing each entry to its waker.
   *
   * '''Supervised, because a dead listener is silent.''' A failed read is retried rather than killing the
   * fiber: a stopped listener leaves every waiter here parked beside work that is ready.
   *
   * '''A failure re-announces everything before retrying.''' Entries can be trimmed while a reader is away
   * and `XREAD` does not report stepping over any, so after a failure the safe assumption is that something
   * was missed — every waker served is told to re-look. The backoff is short: it is the one interval a
   * waiter actually waits on.
   *
   * @return never completes; aborts when a partition has no connection to read on
   */
  def run: IO[RedisFailure, Nothing] =
    ZIO.foreachParDiscard(layout.partitionIds)(loop) *> ZIO.never

  /**
   * One partition's read loop, forever.
   *
   * @param partition whose wake stream to read, on the connection opened for it
   * @return never completes; aborts when no connection was opened for that partition
   */
  private def loop(partition: KeyLayout.Partition): IO[RedisFailure, Unit] =
    poll(partition)
      .flatMap(announce)
      .catchAll: error =>
        // Announce-all is the safe recovery for a missed read — but a reader that lands here every round
        // has degraded into interval polling, which the log line is here to make visible.
        ZIO.logWarning(s"wake read failed, announcing the partition as recovery: ${error.message}")
          *> announceAll *> ZIO.sleep(ReadinessListener.retryBackoff)
      .forever

  /**
   * One `XREAD` on this stream, resuming from where it was left.
   *
   * @param partition the partition whose wake stream to read, on the connection opened for it
   * @return what the entries announce; one naming no known kind is dropped
   */
  private def poll(partition: KeyLayout.Partition): IO[RedisFailure, Chunk[ReadinessListener.Kind]] =
    positions.get.flatMap: current =>
      // `from[String]` pins what Lettuce is handed: an Array[StreamOffset[RedisKey]] would not be an
      // Array[StreamOffset[String]], arrays being invariant.
      val stream = layout.wakeStream(partition)
      val from   = current.getOrElse(stream, ReadinessListener.EntryId("0-0"))
      val offset = StreamOffset.from[String](stream, from)
      entries(partition, offset).flatMap: delivered =>
        val woken = Chunk.fromIterable(delivered.flatMap(ReadinessListener.wakeOf))
        val ahead = delivered.map(entry => RedisKey(entry.getStream) -> ReadinessListener.EntryId(entry.getId)).toMap
        positions.update(_.map((stream, id) => stream -> ahead.getOrElse(stream, id))).as(woken)

  /**
   * The raw read.
   *
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
          .attemptBlocking:
            val arg = XReadArgs.Builder.block(block.toMillis).count(ReadinessListener.count)
            commands.xread(arg, offset)
          .mapBoth(
            LuaScript.failure,
            reply => Option(reply).map(_.asScala.toList).getOrElse(Nil),
          )

  /**
   * Record where each stream this listener will read stands right now.
   *
   * Taken from the same stream set the reads are issued with, so what is positioned is exactly what will be
   * read.
   *
   * @return noop; aborts with `Unavailable` when a position cannot be read
   */
  private def positioned: IO[RedisFailure, Unit] =
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
  ): ZIO[Connection.Commands, RedisFailure, (RedisKey, ReadinessListener.EntryId)] =
    Connection.use: commands =>
      ZIO
        .attemptBlocking(commands.xrevrange(stream, Range.unbounded[String](), Limit.create(0, 1)))
        .mapBoth(
          LuaScript.failure,
          reply =>
            stream -> ReadinessListener.EntryId(
              Option(reply).map(_.asScala.toList).getOrElse(Nil).headOption.map(_.getId).getOrElse("0-0")
            ),
        )

  /**
   * Deliver each wake to its waker, once.
   *
   * @param woken what this batch announced, one per entry
   * @return noop
   */
  private def announce(woken: Chunk[ReadinessListener.Kind]): UIO[Unit] =
    ZIO.foreachDiscard(woken.distinct)(deliver)

  /**
   * Deliver one wake to the sink its kind belongs to.
   *
   * @param wake what was announced
   * @return noop
   */
  private def deliver(wake: ReadinessListener.Kind): UIO[Unit] =
    wake match
      case ReadinessListener.Kind.Queue(name) => queueReady.ready(name)
      case ReadinessListener.Kind.Lock(name)  => lockReady.ready(name)

  /**
   * Tell every waker to re-look — the failure path, where a wake may have been missed and the safe move is
   * to assume so.
   *
   * Both, not just the kind this read was carrying: a stream carries either, so a failed read could have
   * been carrying either.
   *
   * @return noop
   */
  private def announceAll: UIO[Unit] = queueReady.readyAll *> lockReady.readyAll


object ReadinessListener:

  /**
   * The most entries one read may return — a cap on a single reply, not a batch to fill. A high count
   * costs nothing in latency (a read returns as soon as one entry exists) and buys catch-up for a reader
   * that fell behind.
   */
  private val count: Long = 1000

  /** How long to wait after a failed read; short, because a retrying listener is a listener hearing nothing. */
  private val retryBackoff: Duration = 200.millis

  /**
   * Where a reader has got to in a stream: the id of the last entry it was handed.
   *
   * A stream's name and a position in it are both strings, and they travel together — in the positions
   * map, and in the `XREAD` offset that pairs them. Naming this one is what stops the pair being passed
   * the wrong way round, which would compile and resume from a position that means nothing.
   */
  type EntryId = EntryId.Type

  object EntryId:

    opaque type Type <: String = String

    /**
     * An id, trusted.
     *
     * @param value the id as Redis reported it, or `0-0` for a stream nothing has been appended to
     * @return the id
     */
    def apply(value: String): Type = value

  /**
   * A wake as one entry states it: which kind of thing it names, and the name — held at the type that kind
   * implies, so neither case can carry the other's.
   *
   * Every partition has one wake stream and both kinds announce on it, because a wake is written in the
   * same call as the state it announces and that state decides the slot. So the stream an entry arrived on
   * says nothing about what it is; this is what the entry says.
   */
  enum Kind:

    /**
     * A queue that may have work.
     *
     * @param name the queue
     */
    case Queue(name: QueueName)

    /**
     * A lock that came free.
     *
     * @param name the lock
     */
    case Lock(name: LockName)

  object Kind:

    /**
     * The wake an entry's two fields state.
     *
     * '''The tokens are part of the schema.''' `q` and `l` are what the Lua writes into stored entries, so
     * a change to either is a change an older instance would misread, and `KeyLayout.schemaVersion` goes
     * with it.
     *
     * @param kind the entry's `kind` field
     * @param name the entry's `name` field
     * @return the wake, absent when no kind goes by that token
     */
    def read(kind: String, name: String): Option[Kind] = kind match
      case "q" => Some(Queue(QueueName(name)))
      case "l" => Some(Lock(LockName(name)))
      case _   => None

  /**
   * What an entry announces: the kind of thing, and its name.
   *
   * Both fields are required, so an entry missing either — or naming a kind this code does not have — is
   * not something this instance can deliver, and is reported as nothing to deliver.
   *
   * @param entry one stream entry
   * @return what it announces, absent when either field cannot be read
   */
  private def wakeOf(entry: StreamMessage[String, Array[Byte]]): Option[Kind] =
    for
      body <- Option(entry.getBody)
      kind <- Option(body.get("kind")).map(field)
      name <- Option(body.get("name")).map(field)
      wake <- Kind.read(kind, name)
    yield wake

  /**
   * One entry field, as it was written.
   *
   * @param bytes the field's value
   * @return its text
   */
  private def field(bytes: Array[Byte]): String = String(bytes, "UTF-8")

  /**
   * A listener over the given wake streams, each positioned at its end, routing entries to their readiness.
   *
   * '''"From now" is resolved here, to a concrete id.''' Storing the `$` that means "the end of the stream"
   * would be a bug: it is re-evaluated by each read, so anything appended between two reads would be stepped
   * over. Resolving once means every read asks for "after the last entry I actually saw".
   *
   * @param connection the blocking connections to read on, and the shared one that resolves the positions
   * @param block how long one read waits before going round again
   * @param layout which partitions there are, and the stream each one announces on
   * @param queueReady where queue wakes go
   * @param lockReady where lock wakes go
   * @return the listener; aborts with `Unavailable` when a stream's position cannot be read
   */
  def make(
    connection: Connection,
    block: Duration,
    layout: KeyLayout,
    queueReady: QueueReadiness,
    lockReady: LockReadiness,
  ): IO[RedisFailure, ReadinessListener] =
    for
      positions <- Ref.make(Map.empty[RedisKey, EntryId])
      listener   = ReadinessListener(connection, layout, queueReady, lockReady, positions, block)
      _         <- listener.positioned
    yield listener
