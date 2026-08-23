-- Finish a claim that BLMOVE already started, and hand back the message to work on.
--
-- Runs *after* `BLMOVE ready claiming LEFT RIGHT <timeout>` — the blocking part cannot live in a
-- script, so the key sits in `claiming` for the instant between the two. A worker that dies there
-- leaves the key in `claiming`, which the watchdog sweeps.
--
-- KEYS[1] claiming   list  keys handed to a worker, mid-transition
-- KEYS[2] state      hash
-- KEYS[3] claimed    zset  key -> deadline (unix millis); the lease
-- KEYS[4] fence      hash  key -> monotonic claim counter
-- KEYS[5] msgs       list
-- KEYS[6] inflight   list  the one message being worked
-- ARGV[1] key
-- ARGV[2] now        unix millis
-- ARGV[3] ttl        millis
-- returns            {message, token}, or nil when the key had nothing left
local claiming, state, claimed, fence, msgs, inflight = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
local key, now, ttl = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])

redis.call('LREM', claiming, 1, key)

-- LEFT: oldest first. RPUSH in produce.lua plus a LEFT pop here is FIFO; popping RIGHT would make
-- it a stack and silently reverse the per-key ordering this design exists to guarantee.
local message = redis.call('LMOVE', msgs, inflight, 'LEFT', 'RIGHT')

if not message then
  redis.call('HDEL', state, key)   -- nothing to do after all; back to idle rather than held
  return nil
end

redis.call('HSET', state, key, 'processing')
redis.call('ZADD', claimed, now + ttl, key)

-- The deadline exists from this instant, so a key is never in flight without one. The token is what
-- makes a later reclaim safe: a zombie worker's completion is rejected because its token is stale.
return { message, redis.call('HINCRBY', fence, key, 1) }
