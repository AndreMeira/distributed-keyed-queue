package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.{ LockName, QueueName }
import homelab.keyedqueue.infrastructure.redis.keys.{ KeyLayout, RedisKey }
import homelab.keyedqueue.infrastructure.redis.script.LuaScript
import io.lettuce.core.XReadArgs.StreamOffset
import io.lettuce.core.{ Limit, Range, XReadArgs }
import zio.*

import scala.jdk.CollectionConverters.*


/**
 * A blocking read per group of wake streams, delivering each entry to the readiness that waits on it.
 *
 * '''One fiber per group, because the cluster bounds what one read may name.''' `XREAD` is multi-stream,
 * but Redis Cluster rejects a multi-key read whose keys span slots — and the partition tags exist precisely
 * to spread slots. [[Connection]] therefore opens a connection per group of streams that one command may
 * name, and this runs a fiber on each; on a single server there is one group, so one connection and one
 * fiber — the original design. Which readiness an entry reaches is decided by the kind the '''entry'''
 * carries, not by the stream it arrived on: every kind of wake for a partition shares that partition's
 * stream, so queue wakes and lock wakes stay separate without costing a stream, a slot and a connection
 * each. Reading an entry produces a [[ReadinessListener.Kind]], which holds the name at the type its kind
 * implies, so the readiness a wake goes to and the type it arrives as are decided together.
 *
 * '''The two readinesses answer opposite questions, which is why delivery is a match and not a table.''' A
 * [[QueueReadiness]] keeps a wake nobody is waiting for — work does not stop existing because no consumer
 * happened to be parked — while a [[LockReadiness]] drops it and wakes every parked waiter, since grants go
 * by ticket and only the store knows whose turn it is. Sending either one's wakes to the other would lose
 * the property it depends on, and neither will take the other's name.
 *
 * '''The set of streams is fixed, and that is the point.''' An `XREAD` names the streams it was issued
 * with, so the set is resolved once at startup — a queue or lock nobody has asked for yet is still heard
 * the instant something is appended for it, with no read re-issued.
 *
 * '''Why a stream and not pub/sub.''' A reader that reconnects resumes from the id it holds, so a blip
 * costs nothing; pub/sub would lose whatever arrived while it was away, and a lost wake is a waiter asleep
 * beside work it asked for.
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
   * @return never completes; aborts with `Unavailable` when a group has no connection to read on, which is
   *         a startup mistake rather than a read that failed
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
    connection.provideBlocking(partition)(reading(layout.wakeStream(partition)))

  /**
   * Read this stream on the connection in the environment, forever.
   *
   * @param stream what the `XREAD` names
   * @return never completes
   */
  private def reading(stream: RedisKey): ZIO[Connection.Commands, Nothing, Unit] =
    poll(stream)
      .flatMap(announce)
      .catchAll: error =>
        // Announce-all is the safe recovery for a missed read — but a reader that lands here every round
        // has degraded into interval polling, which the log line is here to make visible.
        ZIO.logWarning(s"wake read failed, announcing the group as recovery: ${error.message}")
          *> announceAll *> ZIO.sleep(ReadinessListener.retryBackoff)
      .forever

  /**
   * One `XREAD` on this stream, resuming from where it was left.
   *
   * @param stream the stream it reads
   * @return what the entries announce; one naming no known kind is dropped
   */
  private def poll(stream: RedisKey): ZIO[Connection.Commands, RedisFailure, Chunk[ReadinessListener.Kind]] =
    positions.get.flatMap: current =>
      // `from[String]` pins what Lettuce is handed: an Array[StreamOffset[RedisKey]] would not be an
      // Array[StreamOffset[String]], arrays being invariant.
      val from    = current.getOrElse(stream, ReadinessListener.EntryId("0-0"))
      val offsets = Array(StreamOffset.from[String](stream, from))
      entries(offsets).flatMap: delivered =>
        val woken = Chunk.fromIterable(delivered.flatMap(ReadinessListener.wakeOf))
        val ahead = delivered.map(entry => RedisKey(entry.getStream) -> ReadinessListener.EntryId(entry.getId)).toMap
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
    Connection.use: commands =>
      ZIO
        .attemptBlocking:
          val arg = XReadArgs.Builder.block(block.toMillis).count(ReadinessListener.count)
          commands.xread(arg, offsets*)
        .mapBoth(
          LuaScript.failure,
          reply => Option(reply).map(_.asScala.toList).getOrElse(Nil),
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
   * Every waker, not this group's: a stream carries every kind, so a read that failed could have been
   * carrying any of them.
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
  private def wakeOf(entry: io.lettuce.core.StreamMessage[String, Array[Byte]]): Option[Kind] =
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
    positioned(connection, layout.wakeStreamKeys.toChunk)
      .map(ReadinessListener(connection, layout, queueReady, lockReady, _, block))

  /**
   * Where each stream this listener will read stands right now.
   *
   * Given the same stream set the reads are built from, so what is positioned is exactly what will be read
   * — there is no second opinion about which streams exist.
   *
   * @param connection whose shared connection answers
   * @param streams every stream this will read
   * @return the positions, ready to read from; aborts with `Unavailable` when one cannot be read
   */
  private def positioned(
    connection: Connection,
    streams: Chunk[RedisKey],
  ): IO[RedisFailure, Ref[Map[RedisKey, EntryId]]] =
    ZIO
      .foreach(streams)(stream => position(connection.sync, stream).map(stream -> _))
      .flatMap(resolved => Ref.make(resolved.toMap))

  /**
   * Where a wake stream is right now: the id of its last entry, or `0-0` when nothing has been appended.
   *
   * Asked on the shared connection, not a group's: this answers at once, and the groups are for commands
   * that block.
   *
   * @param commands the connection to ask on
   * @param stream the wake stream
   * @return the id to read after; aborts with `Unavailable` when the read fails
   */
  private def position(commands: Connection.Commands, stream: RedisKey): IO[RedisFailure, EntryId] =
    ZIO
      .attemptBlocking(commands.xrevrange(stream, Range.unbounded[String](), Limit.create(0, 1)))
      .mapBoth(
        LuaScript.failure,
        reply => EntryId(Option(reply).map(_.asScala.toList).getOrElse(Nil).headOption.map(_.getId).getOrElse("0-0")),
      )
