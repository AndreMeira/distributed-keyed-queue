package homelab.keyedqueue.infrastructure.redis

/**
 * A key this adapter addresses Redis by.
 *
 * Named rather than left as `String` for the reason [[homelab.keyedqueue.infrastructure.redis.script.LuaScript.Sha]]
 * is: the strings this adapter passes to Redis are not interchangeable, and the ones that are not keys —
 * a prefix a script builds names from, a stream's entry id — would be accepted anywhere a key is expected
 * and fail as a lookup of something that was never written.
 *
 * '''`RedisKey`, not `Key`.''' A bare `Key` in this codebase reads as the message key, which is the thing
 * the whole product is about; the redundancy with the package is the smaller cost.
 *
 * The `<:` keeps it usable as a `String` wherever Redis is actually called. Note that arrays are invariant,
 * so an `Array[RedisKey]` is not an `Array[String]`: the static KEYS arrays a script is called with stay
 * annotated as `Array[String]`, holding keys.
 */
type RedisKey = RedisKey.Type


object RedisKey:

  opaque type Type <: String = String

  /**
   * A key, trusted.
   *
   * @param value the key as it will be sent to Redis
   * @return the key
   */
  def apply(value: String): Type = value
