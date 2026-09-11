package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.*
import zio.{ Chunk, NonEmptyChunk }


/**
 * Every Redis key one queue owns, derived from its name and the partition it falls in.
 *
 * '''The hash tag is the partition, not the queue.''' A Lua script may only touch keys in one cluster slot,
 * and the scripts build some of their key names at runtime from `prefix`, so everything a script touches
 * has to hash together — including the stream that announces them. Tagging by partition puts a queue's keys and the
 * stream that announces them in one slot, exactly as tagging by queue did, while letting many queues share
 * one wake stream.
 *
 * '''Every key carries the schema version''' — `{p:3}:v1:q:jobs:ready` — after the tag, deliberately: a
 * key's incarnations under different schemas share a slot, so a future migration step can move state
 * between versions atomically in one script. The version makes any schema's leftovers findable by pattern
 * without knowing its shapes (`docs/research/schema-versioned-keys.md`).
 *
 * '''Why share a wake stream at all.''' A listener's `XREAD` names the streams it was issued with, so a
 * per-queue stream means the set of streams grows as queues are served, and a queue added while a read is
 * in flight goes unheard until that read returns. A fixed set of partitions is heard from the first read
 * onwards, which takes the block off the latency path entirely.
 *
 * @param queue the queue these keys belong to
 */
final case class Namespace(queue: QueueName):

  /** Which partition this queue falls in, and therefore which slot and which wake stream it uses. */
  val partition: Int = Namespace.partitionOf(queue)

  /** The tag every key shares, and what the scripts rebuild the per-key names from. */
  val prefix: String = s"${Namespace.tag(partition)}:${KeyLayout.segment}:q:$queue"

  /**
   * Keys with work and nobody working them, scored by when each became claimable.
   */
  val ready: String = s"$prefix:ready"

  /**
   * The counter that scores [[ready]]: one number per key that becomes claimable, ever increasing.
   *
   * '''Arrival order, not a clock.''' A timestamp would be the obvious score and is wrong here — at even
   * moderate rates many keys become claimable inside the same millisecond, and `ZPOPMIN` breaks a tie by
   * member name, so cross-key ordering would quietly become alphabetical. Measured on this codebase, 200
   * keys enqueued back to back produced 133 distinct millisecond scores. A counter has no ties by
   * construction, and it makes every writer agree on what "older" means without agreeing on a clock.
   */
  val sequence: String = s"$prefix:seq"

  /** key -> lease deadline, in unix millis. */
  val claimed: String = s"$prefix:claimed"

  /** key -> claim generation. A token authorises exactly one transition. */
  val fence: String = s"$prefix:fence"

  /** message id -> how many times it has been delivered. Per message, since a claim may own several. */
  val attempts: String = s"$prefix:attempts"

  /**
   * The stream this queue announces on: one entry per key made claimable, appended by the same script
   * that made it so, and shared with every other queue in the partition.
   */
  val wake: String = Namespace.wake(partition)

  /** key -> when a failed message may be retried. */
  val delayed: String = s"$prefix:delayed"

  /**
   * That key's message ids, in producer order, until they are acknowledged.
   *
   * The list carries order and nothing else — [[payloads]] carries the messages. A claim marks ids as
   * [[owned]] without taking them out of here, which is why a nack has nothing to put back.
   *
   * @param key the key
   * @return the list name
   */
  def msgs(key: MessageKey): String = s"$prefix:msgs:$key"

  /**
   * That key's messages themselves, by id.
   *
   * Separate from [[msgs]] because the two answer different questions: the list carries order, this carries
   * cargo. Keeping them apart is what lets a script address a message by name without reading inside a
   * payload it cannot parse.
   *
   * @param key the key
   * @return the hash name
   */
  def payloads(key: MessageKey): String = s"$prefix:payloads:$key"

  /**
   * The ids this key's live claim owns and has not yet settled.
   *
   * A claim is over when this is empty. Ownership is the only thing it records — the messages themselves
   * stay in [[msgs]] in producer order for as long as they are unacknowledged, so a nack has nothing to put
   * back and a crash has nothing to recover.
   *
   * @param key the key
   * @return the set name
   */
  def owned(key: MessageKey): String = s"$prefix:owned:$key"


object Namespace:

  /**
   * How many partitions the deployment is divided into — fixed in code, not configured.
   *
   * The count is a ceiling on spread (at most this many cluster nodes ever hold this service's data) but a
   * floor on overhead (the listener names every partition's stream in every read, maintenance iterates every
   * partition), so it is one number chosen once: high enough that no realistic cluster hits the ceiling, low
   * enough that the floor stays invisible. Sixteen node ceiling; sixteen-stream reads.
   *
   * '''Part of the schema.''' Changing it moves queues between tags and strands whatever was written under
   * the old count, so a change here is a change to the shape of stored data — bump
   * `KeyLayout.schemaVersion` with it, and the boot check turns the stranding into a refusal.
   */
  val partitions: Int = 16

  /**
   * The hash tag a partition's keys share.
   *
   * @param partition the partition
   * @return the tag, braces included, so Redis hashes only what is inside them
   */
  def tag(partition: Int): String = s"{p:$partition}"

  /**
   * A partition's wake stream.
   *
   * @param partition the partition
   * @return the stream name
   */
  def wake(partition: Int): String = s"${tag(partition)}:${KeyLayout.segment}:wake"

  /**
   * Which partition a queue falls in.
   *
   * `String`'s hash is specified by the JVM, so every instance agrees on where a queue lives without being
   * told. `floorMod`, because a negative hash would otherwise produce a negative partition.
   *
   * @param queue the queue
   * @return the partition
   */
  def partitionOf(queue: QueueName): Int = Math.floorMod(queue.toString.hashCode, partitions)

  /**
   * Every wake stream in the deployment — the fixed set a listener reads.
   *
   * Fixed is the point: the set is known before any queue is served, so no read is ever re-issued because
   * a consumer arrived for a queue nobody had asked for yet.
   *
   * @return the stream names, in partition order
   */
  val wakeStreams: NonEmptyChunk[String] =
    NonEmptyChunk.fromChunk(Chunk.fromIterable(0 until partitions).map(wake)).getOrElse(NonEmptyChunk(wake(0)))
