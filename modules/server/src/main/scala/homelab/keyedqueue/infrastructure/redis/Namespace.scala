package homelab.keyedqueue.infrastructure.redis


import homelab.keyedqueue.domain.types.*
import zio.{ Chunk, NonEmptyChunk }


/**
 * Every Redis key one queue owns, derived from its name and the bucket it falls in.
 *
 * '''The hash tag is the bucket, not the queue.''' A Lua script may only touch keys in one cluster slot,
 * and the scripts build some of their key names at runtime from `prefix`, so everything a script touches
 * has to hash together — including the stream that announces them. Tagging by bucket puts a queue's keys and the
 * stream that announces them in one slot, exactly as tagging by queue did, while letting many queues share
 * one wake stream.
 *
 * '''Why share a wake stream at all.''' A listener's `XREAD` names the streams it was issued with, so a
 * per-queue stream means the set of streams grows as queues are served, and a queue added while a read is
 * in flight goes unheard until that read returns. A fixed set of buckets is heard from the first read
 * onwards, which takes the block off the latency path entirely.
 *
 * At `buckets == 1` there is one tag, one wake stream and one slot — the simplest deployment, and the
 * whole service on one node. Above 1, queues spread across slots and each bucket carries its own stream.
 *
 * @param queue the queue these keys belong to
 * @param buckets how many buckets the deployment is divided into; fixed for its life, since changing it
 *                moves queues between tags and strands whatever was written under the old one
 */
final case class Namespace(queue: QueueName, buckets: Int):

  /** Which bucket this queue falls in, and therefore which slot and which wake stream it uses. */
  val bucket: Int = Namespace.bucketOf(queue, buckets)

  /** The tag every key shares, and what the scripts rebuild the per-key names from. */
  val prefix: String = s"${Namespace.tag(bucket)}:q:$queue"

  /** Keys with work and nobody working them. The only place anything blocks. */
  val ready: String = s"$prefix:ready"

  /** key -> queued | processing. Absence means idle, which keeps the hash small. */
  val state: String = s"$prefix:state"

  /** key -> lease deadline, in unix millis. */
  val claimed: String = s"$prefix:claimed"

  /** key -> claim generation. A token authorises exactly one transition. */
  val fence: String = s"$prefix:fence"

  /** message id -> how many times it has been delivered. Per message, since a claim may own several. */
  val attempts: String = s"$prefix:attempts"

  /**
   * The stream this queue announces on: one entry per key made claimable, appended by the same script
   * that made it so, and shared with every other queue in the bucket.
   *
   * A stream rather than a pub/sub channel because a reader that reconnects resumes from the id it holds
   * instead of losing what it missed. Entries name the queue they concern, because the stream no longer
   * does.
   */
  val wake: String = Namespace.wake(bucket)

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
   * The hash tag a bucket's keys share.
   *
   * @param bucket the bucket
   * @return the tag, braces included, so Redis hashes only what is inside them
   */
  def tag(bucket: Int): String = s"{w:$bucket}"

  /**
   * A bucket's wake stream.
   *
   * @param bucket the bucket
   * @return the stream name
   */
  def wake(bucket: Int): String = s"${tag(bucket)}:wake"

  /**
   * Which bucket a queue falls in.
   *
   * `String`'s hash is specified by the JVM rather than left to an implementation, so every instance of the
   * service agrees on where a queue lives without being told — which is the only property this needs, since
   * nothing outside the service computes it. `floorMod` rather than `%` because a negative hash would
   * otherwise produce a negative bucket.
   *
   * @param queue the queue
   * @param buckets how many buckets the deployment is divided into
   * @return the bucket
   */
  def bucketOf(queue: QueueName, buckets: Int): Int = Math.floorMod(queue.toString.hashCode, buckets)

  /**
   * Every wake stream in the deployment — the fixed set a listener reads.
   *
   * Fixed is the point: the set is known before any queue is served, so no read is ever re-issued because
   * a consumer arrived for a queue nobody had asked for yet.
   *
   * @param buckets how many buckets the deployment is divided into; at least one
   * @return the stream names, in bucket order
   */
  def wakeStreams(buckets: Int): NonEmptyChunk[String] =
    NonEmptyChunk.fromChunk(Chunk.fromIterable(0 until buckets).map(wake)).getOrElse(NonEmptyChunk(wake(0)))
