-- Release a lock, if the caller still holds it.
--
-- KEYS[1] held, KEYS[2] fence
-- ARGV[1] name, ARGV[2] token
-- returns 1 when released, 0 when the token is stale (the lock was reclaimed or already released)
--
-- The fence advances so the released token cannot act again — the same guard the queue's settle uses to
-- retire a claim. A release racing a reclaim loses cleanly: the reclaim advanced the fence first, so this
-- sees a mismatch and does nothing.
local held, fence = KEYS[1], KEYS[2]
local name, token = ARGV[1], tonumber(ARGV[2])

if tonumber(redis.call('HGET', fence, name) or 0) ~= token then
  return 0
end

redis.call('HINCRBY', fence, name, 1)
redis.call('ZREM', held, name)
return 1
