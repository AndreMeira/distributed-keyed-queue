package homelab.keyedqueue.infrastructure.redis.keys


import homelab.common.error.ApplicationError
import homelab.keyedqueue.domain.types.{ LockName, QueueName }
import homelab.keyedqueue.infrastructure.configuration.Misconfigured
import homelab.keyedqueue.infrastructure.redis.{ Connection, RedisFailure }
import io.lettuce.core.SetArgs
import zio.*

import java.nio.charset.StandardCharsets


/**
 * How one deployment divides its keys: which partition a name falls in, the keys that follow from it, and
 * which of them a single command may name together.
 *
 * '''The partition of a name must not depend on the deployment.''' It is written into every key as a hash
 * tag, so an instance computing it differently would look for keys in a partition they were never written
 * to — and nothing would report it, the schema marker being about shape rather than placement.
 *
 * '''A partition is the unit of everything here.''' It decides a name's hash tag, and so its slot; it owns
 * exactly one wake stream; and it is what a blocking read is opened for. Nothing in it depends on whether
 * the store is a cluster — a single-key read is legal on either.
 *
 * @param partitions how many partitions this deployment is divided into
 */
final case class KeyLayout(partitions: Int):

  /** Every partition, in order. */
  val partitionIds: Chunk[KeyLayout.Partition] =
    Chunk
      .fromIterable(0 until partitions)
      .map(KeyLayout.Partition(_))

  /** Each partition's tag, in partition order. */
  private val tags: Chunk[KeyLayout.Tag] = partitionIds.map(KeyLayout.Tag(_))

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
   * Check this layout against the one the store was written under, recording it on a first boot.
   *
   * @return noop; aborts with `Misconfigured` when the store was written under a different layout, and
   *         with `RedisFailure` when it cannot be reached
   */
  def verify: ZIO[Connection.Commands, ApplicationError, Unit] = KeyLayout.verifying(stamp)

  /**
   * Overwrite the store's record with this layout — the `layout accept` run mode.
   *
   * @return noop; aborts with `RedisFailure` when the store cannot be reached
   */
  def accept: ZIO[Connection.Commands, ApplicationError, Unit] = KeyLayout.accepting(stamp)

  /**
   * This layout as the marker stores it: the schema version, and the partition count.
   *
   * '''The count is in it because it is no longer the same everywhere.''' A cluster spreads across
   * [[KeyLayout.partitions]] of them and a single server uses one, so two instances disagreeing about which
   * they are would write the same names into different tags — and a version alone could not tell.
   *
   * @return the stamp
   */
  private def stamp: String = s"v${KeyLayout.schemaVersion}.p$partitions"

  /**
   * A partition's wake stream.
   *
   * @param partition the partition
   * @return its stream
   */
  def wakeStream(partition: KeyLayout.Partition): RedisKey =
    KeyLayout.wakeStreamKey(KeyLayout.Tag(partition))


/**
 * The store's record of the schema it was written under, and the boot-time check against it.
 *
 * The shape of everything this code stores — the structures, their encodings, and the partition constant that
 * decides every key's hash tag — is a property of the code, while the keys outlive every instance. Running
 * new-shaped code against old-shaped data does not degrade, it fails in ways no error message will
 * explain: a structure read as the wrong type, a key looked for in a partition it was never written to. This
 * records the schema version in the store itself, where the data is, and refuses to start an instance
 * whose code disagrees.
 */
object KeyLayout:

  /** Where the schema version is recorded. Deliberately version-less: it is the fence every version reads. */
  private val schema: RedisKey = RedisKey("dkq:layout:schema")

  /**
   * The shape of everything this code stores: the queue's and the lock's structures, the encodings written
   * into them, and [[partitions]]. '''Bump it on any change an older instance would misread''' — a
   * structure changing type, a field changing meaning, an encoding changing form, the partition constant
   * changing. That is a review discipline, not something the code can detect; an unbumped version makes
   * the check vouch for a compatibility that is not there.
   *
   * Gate-only, deliberately: a mismatch is refused, never migrated, and the remedy is always drain or
   * flush, then `layout accept`. What it does '''not''' cover is client-held state — a receipt format
   * change breaks holds the store never sees, and no store-side marker can catch it.
   *
   * '''Written into every key''' as [[segment]], so a version's state stays findable by pattern long after
   * the code that shaped it is gone — the property a future migration worker stands on
   * (`docs/research/schema-versioned-keys.md`).
   */
  val schemaVersion: Int = 3

  /** The schema version as every key carries it, between the hash tag and the rest of the name. */
  val segment: String = s"v$schemaVersion"

  /**
   * How many partitions a deployment is divided into unless it says otherwise.
   *
   * The count is a ceiling on spread (at most this many cluster nodes ever hold this service's data) but a
   * floor on overhead (the listener names every partition's stream in every read and holds a connection per
   * slot, maintenance iterates every partition), so it is one number chosen once: high enough that no
   * realistic cluster hits the ceiling, low enough that the floor stays invisible.
   *
   * '''One count for every kind of name.''' Queues and locks share a partition, and with it a hash tag and
   * a wake stream. They have no choice: a wake is written in the same call as the state it announces, so
   * the stream can only live in the tag that state lives in.
   *
   * '''Part of the schema.''' Changing it moves names between tags and strands whatever was written under
   * the old count, so a change here is a change to the shape of stored data — bump [[schemaVersion]] with
   * it, and the boot check turns the stranding into a refusal. What the check covers is this '''constant''',
   * not the count an instance was built with: two instances given different counts would strand each
   * other's keys with nothing to notice, so a count that ever comes from configuration has to be recorded
   * in the marker alongside the version.
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
   * '''A single server uses one.''' Partitions exist to spread keys across cluster slots, and a server with
   * no slots has nothing to spread across — so the sixteen would buy nothing and cost a blocking connection
   * each. A cluster takes [[partitions]].
   *
   * @param cluster whether the store is a Redis Cluster
   * @return the layout
   */
  def of(cluster: Boolean): KeyLayout = KeyLayout(if cluster then partitions else 1)

  /**
   * Check this code's schema against the store's, recording it on a first boot.
   *
   * `SET NX` claims the marker atomically, so two first-boot instances racing with different versions
   * cannot both write: one records, the other reads what was recorded and refuses.
   *
   * @return noop; aborts with `Misconfigured` when the store was written under a different schema, and
   *         with `RedisFailure` when it cannot be reached
   */
  private def verifying(stamp: String): ZIO[Connection.Commands, ApplicationError, Unit] =
    claimed(stamp).flatMap:
      case None                                => ZIO.unit // first boot: this code's schema is now the store's
      case Some(recorded) if recorded == stamp => ZIO.unit
      case Some(recorded)                      =>
        ZIO.fail:
          Misconfigured:
            s"this store was written under layout $recorded, but this instance is $stamp. " +
              "The stored structures have a different shape than this code expects, and running against " +
              "them fails in ways no error message will explain. If the store has been drained (or " +
              "flushed) on purpose, run the 'layout accept' mode once to record the new schema."

  /**
   * Overwrite the store's record with this code's schema — the `layout accept` run mode.
   *
   * The deliberate half of the ceremony the boot check enforces — and it '''trusts the ceremony''': it
   * records over whatever is there. It cannot verify the drain, because what a leftover key looks like
   * depends on the schema being replaced, which this code no longer knows; and it cannot see instances,
   * since a running one re-checks nothing after boot. Both halves of the ceremony — drain first, no
   * instances attached — are the operator's, stated in `docs/architecture/redis-cluster.md`.
   *
   * @return noop; aborts with `RedisFailure` when the store cannot be reached
   */
  private def accepting(stamp: String): ZIO[Connection.Commands, ApplicationError, Unit] =
    for
      previous <- recorded
      _        <- record(stamp)
      _        <- ZIO.logInfo:
                    s"layout accepted: schema ${previous.getOrElse("unset")} -> $schemaVersion. " +
                      "Start instances only now: a running instance checks its layout at boot and never again."
    yield ()

  /**
   * `SET NX` the marker, reading what is there when the claim loses.
   *
   * @return `None` when this call recorded it; the recorded text when someone already had
   */
  private def claimed(stamp: String): ZIO[Connection.Commands, RedisFailure, Option[String]] =
    attempt(stamp).flatMap:
      case Claim.Recorded    => ZIO.none
      case Claim.Found(text) => ZIO.some(text)
      // Lost the claim, then found nothing: the marker vanished between the two reads. Ask again.
      case Claim.Vanished    => claimed(stamp)

  /**
   * One round of the claim: try to record, and read on losing.
   *
   * @return what happened
   */
  private def attempt(stamp: String): ZIO[Connection.Commands, RedisFailure, Claim] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking {
          if redis.set(schema, utf8(stamp), SetArgs.Builder.nx()) == "OK" then Claim.Recorded
          else
            Option(redis.get(schema)) match
              case Some(bytes) => Claim.Found(String(bytes, StandardCharsets.UTF_8))
              case None        => Claim.Vanished
        }
        .mapError(error => RedisFailure.Unavailable(error.getMessage))

  /** What one round of claiming the marker can find. */
  private enum Claim:

    /** Nobody had recorded it; this call did. */
    case Recorded

    /** Someone recorded this before us. */
    case Found(text: String)

    /** The claim lost, yet the read found nothing — the marker was deleted between the two. */
    case Vanished

  /**
   * What the marker currently records.
   *
   * @return its text, absent when it was never written
   */
  private def recorded: ZIO[Connection.Commands, RedisFailure, Option[String]] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(Option(redis.get(schema)).map(bytes => String(bytes, StandardCharsets.UTF_8)))
        .mapError(error => RedisFailure.Unavailable(error.getMessage))

  /**
   * Write the marker, unconditionally.
   *
   * @return noop
   */
  private def record(stamp: String): ZIO[Connection.Commands, RedisFailure, Unit] =
    Connection.use: redis =>
      ZIO
        .attemptBlocking(redis.set(schema, utf8(stamp)))
        .mapError(error => RedisFailure.Unavailable(error.getMessage))
        .unit

  /**
   * Text as the marker stores it.
   *
   * @param value the text
   * @return its UTF-8 bytes
   */
  private def utf8(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)
