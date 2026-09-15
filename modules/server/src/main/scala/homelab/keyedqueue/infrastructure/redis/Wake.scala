package homelab.keyedqueue.infrastructure.redis

import homelab.keyedqueue.domain.types.{ LockName, QueueName }


/**
 * What a reader of the wake streams hands on: a name that became ready, or the news that it lost its place.
 *
 * '''A gap is a value, not a failure.''' `XREAD` does not report entries trimmed while a reader was away,
 * so a reader that fell off its position knows only that anything may have been missed — and that is
 * something to act on, not something to abort with. Only the reader can notice it; only its consumer knows
 * what to do about it.
 */
enum Wake:

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

  /**
   * The reader lost its place, so every name must be treated as possibly ready.
   *
   * No Redis reader emits one: a stream is replayed from the id the reader holds, so a failed read delays
   * rather than loses. A transport that cannot replay — a notification channel, say — has no other way to
   * recover from a disconnect.
   */
  case Gap


object Wake:

  /**
   * The wake an entry's two fields state.
   *
   * '''The tokens are part of the schema.''' `q` and `l` are what the Lua writes into stored entries, so a
   * change to either is a change an older instance would misread, and `KeyLayout.schemaVersion` goes with
   * it.
   *
   * @param kind the entry's `kind` field
   * @param name the entry's `name` field
   * @return the wake, absent when no kind goes by that token
   */
  def read(kind: String, name: String): Option[Wake] = kind match
    case "q" => Some(Queue(QueueName(name)))
    case "l" => Some(Lock(LockName(name)))
    case _   => None
