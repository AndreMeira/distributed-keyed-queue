-- Finish a claim that BLMOVE already started, and hand back the message to work on.
--
-- Runs *after* `BLMOVE ready claiming:<worker> LEFT RIGHT <timeout>` — the blocking part cannot live in a
-- script, so the key sits in the worker's claiming list for the instant between the two. A worker that dies
-- there is recovered by the watchdog's worker-liveness sweep.
--
-- KEYS[1] claiming   list  this worker's in-transition keys
-- KEYS[2] state      hash  key -> queued | processing (absent == idle)
-- KEYS[3] claimed    zset  key -> deadline (unix millis); the lease
-- KEYS[4] fence      hash  key -> monotonic claim counter
-- KEYS[5] msgs       list  this key's messages, oldest first
-- KEYS[6] inflight   list  the one message being worked
-- KEYS[7] attempts   hash  key -> how many times the CURRENT head message has been delivered
-- ARGV[1] key
-- ARGV[2] ttl        millis
-- returns            {message, token, attempt, deadline, backlog}, or nil when the key had nothing left
local claiming, state, claimed, fence, msgs, inflight, attempts =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7]
local key, ttl = ARGV[1], tonumber(ARGV[2])

redis.call('LREM', claiming, 1, key)

-- LEFT: oldest first. RPUSH in produce.lua plus a LEFT pop here is FIFO; popping RIGHT would make it a
-- stack and silently reverse the per-key ordering this design exists to guarantee.
local message = redis.call('LMOVE', msgs, inflight, 'LEFT', 'RIGHT')

if not message then
  redis.call('HDEL', state, key)   -- nothing to do after all; back to idle rather than held
  return nil
end

-- Server time, so deadlines do not depend on a caller's clock: a fast one would write leases that never
-- expire, a slow one would have the watchdog reclaim live work.
local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

redis.call('HSET', state, key, 'processing')
redis.call('ZADD', claimed, now + ttl, key)

-- Counts deliveries of the message now at the head. Reset by a DONE settle, which is what moves the head;
-- a FAILED settle leaves the head in place, so its count keeps climbing — that is what makes a poison
-- message visible rather than silent.
local attempt = redis.call('HINCRBY', attempts, key, 1)

-- What is still queued for this key, measured after the LMOVE above, so it counts what is BEHIND the
-- message being handed over rather than including it.
--
-- A lower bound rather than a fixed number: the claim keeps other CONSUMERS off the key, but produce.lua
-- RPUSHes to this list whatever the key's state, so a producer may append while the claim is held. It can
-- only grow — nothing but the holder may take from it — which is why a discard counted from the head
-- cannot reach a message that arrived after this was read.
local backlog = redis.call('LLEN', msgs)

return { message, redis.call('HINCRBY', fence, key, 1), attempt, now + ttl, backlog }
