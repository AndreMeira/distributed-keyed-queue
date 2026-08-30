-- Finish a claim that BLMOVE already started, and hand back a batch of this key's messages to work on.
--
-- Runs *after* `BLMOVE ready claiming:<worker> LEFT RIGHT <timeout>` — the blocking part cannot live in a
-- script, so the key sits in the worker's claiming list for the instant between the two. A worker that dies
-- there is recovered by the watchdog's worker-liveness sweep.
--
-- KEYS[1] claiming   list  this worker's in-transition keys
-- KEYS[2] state      hash  key -> queued | processing (absent == idle)
-- KEYS[3] claimed    zset  key -> deadline (unix millis); the lease
-- KEYS[4] fence      hash  key -> monotonic claim counter
-- KEYS[5] msgs       list  this key's message ids, producer order, until acknowledged
-- KEYS[6] payloads   hash  message id -> the message itself
-- KEYS[7] owned      set   the ids this claim owns and has not settled
-- KEYS[8] attempts   hash  message id -> how many times it has been delivered
-- ARGV[1] key
-- ARGV[2] ttl        millis
-- ARGV[3] batch      the most messages to hand over; at least 1
-- returns            {token, deadline, backlog, ids, messages, attempts}, or nil when the key had nothing
--
-- '''Claimed messages do not move.''' They stay in `msgs`, in producer order, and `owned` records which of
-- them this claim holds. That is what makes a nack free — there is nothing to put back — and what makes the
-- watchdog's recovery a matter of forgetting the claim rather than repairing a list.
local claiming, state, claimed, fence, msgs, payloads, owned, attempts =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7], KEYS[8]
local key, ttl, batch = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])

-- The key is in this worker's claiming list because this worker's BLMOVE put it there, and the only other
-- thing that touches that list is the watchdog draining it after the worker's liveness lapsed. So a removal
-- that finds nothing means exactly one thing: this worker was declared dead while it was away, the key has
-- been given back, and somebody else may already hold it.
--
-- Refusing here is what stops two workers running handlers for one key at the same time. Without it, a
-- worker that stalled past its liveness — a long GC pause, a partition — would wake and claim messages
-- another worker is already working. The fence would refuse its settles, but the work would have happened
-- twice, concurrently, which is the one thing this design exists to prevent.
if redis.call('LREM', claiming, 1, key) == 0 then
  return nil
end

-- The head of the list, oldest first. RPUSH in produce.lua plus reading from index 0 here is FIFO; reading
-- from the tail would make it a stack and silently reverse the per-key ordering this design guarantees.
local ids = redis.call('LRANGE', msgs, 0, batch - 1)

if #ids == 0 then
  redis.call('HDEL', state, key)   -- nothing to do after all; back to idle rather than held
  return nil
end

-- Server time, so deadlines do not depend on a caller's clock: a fast one would write leases that never
-- expire, a slow one would have the watchdog reclaim live work.
local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

redis.call('HSET', state, key, 'processing')
redis.call('ZADD', claimed, now + ttl, key)
redis.call('SADD', owned, unpack(ids))

-- Per message, not per key: with several owned at once, "how many times has this been delivered" is a
-- question about a message. A nacked message keeps its count and climbs on redelivery, which is what makes
-- a poison message visible rather than silent.
local counts = {}
for i, id in ipairs(ids) do
  counts[i] = redis.call('HINCRBY', attempts, id, 1)
end

local messages = redis.call('HMGET', payloads, unpack(ids))

-- What is still queued behind the batch: everything in the list the caller was not given.
local backlog = redis.call('LLEN', msgs) - #ids

return { redis.call('HINCRBY', fence, key, 1), now + ttl, backlog, ids, messages, counts }
