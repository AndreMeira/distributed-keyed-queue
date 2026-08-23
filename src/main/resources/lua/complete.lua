-- Retire the in-flight message and decide what happens to its key next.
--
-- KEYS[1] state      hash
-- KEYS[2] claimed    zset
-- KEYS[3] fence      hash
-- KEYS[4] msgs       list
-- KEYS[5] inflight   list
-- KEYS[6] ready      list
-- ARGV[1] key
-- ARGV[2] token      the token handed out by consume.lua
-- returns            1 when applied, 0 when the claim was stale
--
-- The token check is not an optimisation. A missed heartbeat only means the worker cannot be heard,
-- so the watchdog may already have revoked this claim and given the key to somebody else. Without
-- the guard, a zombie would re-queue a key that is already queued — two workers on one key, which
-- is exactly the invariant everything else here protects.
local state, claimed, fence, msgs, inflight, ready = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
local key, token = ARGV[1], tonumber(ARGV[2])

if tonumber(redis.call('HGET', fence, key) or 0) ~= token then
  return 0
end

-- Completing ends the claim, so it must end the token too: a token authorises exactly ONE transition.
-- Without this, a retried `complete` — an at-least-once RPC, a client resending after a timeout — would
-- apply twice and push the key onto `ready` twice, which is two workers on one key.
redis.call('HINCRBY', fence, key, 1)

redis.call('DEL', inflight)
redis.call('ZREM', claimed, key)

if redis.call('LLEN', msgs) > 0 then
  redis.call('HSET', state, key, 'queued')
  redis.call('RPUSH', ready, key)
else
  redis.call('HDEL', state, key)   -- idle is the absence of an entry
end

return 1
