package homelab.keyedqueue.infrastructure.redis.keys


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.types.{ LockName, QueueName }
import homelab.keyedqueue.infrastructure.configuration.Misconfigured
import homelab.keyedqueue.infrastructure.redis.{ Connection, RedisFailure }
import io.lettuce.core.SetArgs
import zio.*

import java.nio.charset.StandardCharsets


/**
 * How one deployment divides its keys: which partition a name falls in, and the keys that follow from it.
 *
 * A partition decides a name's hash tag and so its slot, owns exactly one wake stream, and is what a
 * blocking read is opened for. Which partition a name falls in is written into every key, so every
 * instance of a deployment must compute it the same way — which is what [[verify]] holds them to.
 *
 * @param partitions how many partitions this deployment is divided into
 */
final case class KeyLayout(partitions: Int):

  /** Every partition, in order. */
  val partitionIds: Chunk[KeyLayout.Partition] =
    Chunk.fromIterable(0 until partitions).map(KeyLayout.Partition(_))

  /** Each partition's tag, in partition order. */
  private val tags: Chunk[KeyLayout.Tag] = partitionIds.map(KeyLayout.Tag(_))

  /**
   * This layout as the marker stores it: the schema version, and the partition count.
   *
   * The count is part of the stamp because it varies by deployment: a cluster spreads across
   * [[KeyLayout.partitions]] of them and a single server uses one, and a version alone could not tell two
   * such instances apart.
   *
   * @return the stamp
   */
  private val stamp: String = s"v${KeyLayout.schemaVersion}.p$partitions"

  /**
   * Every partition's lock keys — for the passes that are not about one lock, the trim sweeping them all.
   *
   * @return the keys, in partition order
   */
  val locks: Chunk[LockKeys] = tags.map(LockKeys(_))

  /**
   * Every wake stream in the deployment — the fixed set a listener reads, and so what decides how many
   * connections it holds on a cluster.
   *
   * Fixed is the point: the set is known before any queue or lock is served, so no read is ever re-issued
   * because a caller arrived for a name nobody had asked for yet.
   *
   * @return the stream names, in partition order
   */
  val wakeStreamKeys: NonEmptyChunk[RedisKey] =
    NonEmptyChunk
      .fromChunk(tags.map(KeyLayout.wakeStreamKey))
      .getOrElse(NonEmptyChunk(KeyLayout.wakeStreamKey(KeyLayout.Tag(KeyLayout.Partition(0)))))

  /**
   * Which partition a name falls in, whatever kind of thing it names.
   *
   * `String`'s hash is specified by the JVM, so every instance agrees on where a name lives without being
   * told. `floorMod`, because a negative hash would otherwise produce a negative partition.
   *
   * @param name the queue or lock name
   * @return the partition
   */
  def partitionOf(name: String): KeyLayout.Partition =
    KeyLayout.Partition(Math.floorMod(name.hashCode, partitions))

  /**
   * The keys a queue owns.
   *
   * @param name the queue
   * @return its keys
   */
  def queue(name: QueueName): QueueKeys =
    QueueKeys(KeyLayout.Tag(partitionOf(name)), name)

  /**
   * The keys of the partition a lock falls in.
   *
   * @param name the lock
   * @return its partition's keys
   */
  def lock(name: LockName): LockKeys =
    LockKeys(KeyLayout.Tag(partitionOf(name)))

  /**
   * Check this layout against the one the store was written under, recording it on a first boot.
   *
   * @return noop; aborts with `Misconfigured` when the store was written under a different layout or when
   *         the marker was deleted while this was recording it, and with `RedisFailure` when the store
   *         cannot be reached
   */
  def verify: ZIO[Connection.Commands, ApplicationError, Unit] =
    attempt.flatMap:
      case Some(recorded) if recorded == stamp =>
        ZIO.unit

      case Some(recorded) =>
        ZIO.fail:
          Misconfigured:
            s"this store was written under layout $recorded, but this instance is $stamp. " +
              "The stored structures have a different shape than this code expects, and running against " +
              "them fails in ways no error message will explain. If the store has been drained (or " +
              "flushed) on purpose, delete the 'dkq:layout:schema' key: the next boot records this one."

      case None =>
        ZIO.fail:
          Misconfigured:
            "the layout marker was deleted while this instance was recording it, so what the store holds " +
              "cannot be read. Nothing is wrong with the store — this is a drain racing a start. Start " +
              "the instance again once the drain is done."

  /**
   * A partition's wake stream.
   *
   * @param partition the partition
   * @return its stream
   */
  def wakeStream(partition: KeyLayout.Partition): RedisKey =
    KeyLayout.wakeStreamKey(KeyLayout.Tag(partition))

  /**
   * The layout the store holds once this instance has asked: the one recorded here, or the one already
   * there.
   *
   * `SET NX` claims it atomically, so two first-boot instances racing with different layouts cannot both
   * write: one records, the other reads what was recorded.
   *
   * @return the layout the store holds, this instance's when it recorded it; absent when the marker was
   *         deleted between the claim and the read, which leaves nothing to compare against; aborts with
   *         `Unavailable` when the store cannot be reached
   */
  private def attempt: ZIO[Connection.Commands, RedisFailure, Option[String]] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking {
          if redis.set(KeyLayout.schema, utf8(stamp), SetArgs.Builder.nx()) == "OK" then Some(stamp)
          else Option(redis.get(KeyLayout.schema)).map(bytes => String(bytes, StandardCharsets.UTF_8))
        }
        .mapError(error => RedisFailure.Unavailable(error.getMessage))

  /**
   * Text as the marker stores it.
   *
   * @param value the text
   * @return its UTF-8 bytes
   */
  private def utf8(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)


/**
 * The store's record of the layout it was written under, and the boot-time check against it.
 *
 * Running new-shaped code against old-shaped data does not degrade, it fails in ways no error message will
 * explain — a structure read as the wrong type, a key looked for in a partition it was never written to.
 * So the layout is recorded where the data is, and an instance that disagrees refuses to start.
 */
object KeyLayout:

  /** Where the schema version is recorded. Deliberately version-less: it is the fence every version reads. */
  private val schema: RedisKey = RedisKey("dkq:layout:schema")

  /**
   * The shape of everything this code stores: the structures, and the encodings written into them.
   * Bump it on any change that alters what is stored — a structure changing type, a field changing
   * meaning, an encoding changing form. A review discipline, not something the code can detect.
   *
   * Gate-only: a mismatch is refused, never migrated, and it does not cover client-held state
   * (`docs/architecture/redis-cluster.md`, `docs/research/schema-versioned-keys.md`).
   */
  val schemaVersion: Int = 3

  /** The schema version as every key carries it, between the hash tag and the rest of the name. */
  val segment: String = s"v$schemaVersion"

  /**
   * How many partitions a cluster is divided into; a single server uses one ([[of]]).
   *
   * A ceiling on spread (at most this many nodes ever hold this service's data) against a floor on overhead
   * (a wake stream and a blocking connection each), chosen once. It is part of what the marker records, so
   * an instance that disagrees about it refuses to start (`docs/architecture/redis-cluster.md`).
   */
  val partitions: Int = 16

  /**
   * The hash tag a partition's keys share — the only part of a key Redis hashes, so keys carrying the same
   * one are guaranteed the same slot.
   */
  type Tag = Tag.Type

  object Tag:

    opaque type Type <: String = String

    /**
     * A partition's tag.
     *
     * @param partition the partition
     * @return the tag, braces included
     */
    def apply(partition: Partition): Type = s"{p:$partition}"

  /**
   * One of the slices the keyspace is divided into: a hash tag, one wake stream, and the blocking
   * connection opened to read it.
   */
  type Partition = Partition.Type

  object Partition:

    opaque type Type <: Int = Int

    /**
     * A partition, trusted.
     *
     * @param value which of the partitions this is
     * @return the partition
     */
    def apply(value: Int): Type = value

  /**
   * A partition's wake stream — one per partition, carrying every kind of wake it announces.
   *
   * @param tag the partition's tag
   * @return the stream name
   */
  def wakeStreamKey(tag: Tag): RedisKey = RedisKey(s"$tag:$segment:wake")

  /**
   * The layout of a deployment, which decides how many partitions it wants.
   *
   * A single server uses one partition: they exist to spread keys across cluster slots, and a server has
   * none to spread across. A cluster takes [[partitions]].
   *
   * @param cluster whether the store is a Redis Cluster
   * @return the layout
   */
  def of(cluster: Boolean): KeyLayout = KeyLayout(if cluster then partitions else 1)

  /**
   * The layout of a cluster deployment: every partition, spread across slots.
   *
   * @return the layout
   */
  def cluster: KeyLayout = KeyLayout(partitions)

  /**
   * The layout of a single server: one partition, there being no slots to spread across.
   *
   * @return the layout
   */
  def single: KeyLayout = KeyLayout(1)
