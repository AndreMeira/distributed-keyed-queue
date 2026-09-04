-- Take the next claimable key and hand back a batch of its messages to work on.
--
-- KEYS[1] ready      list  keys with work and nobody working them
-- KEYS[2] state      hash  key -> queued | processing (absent == idle)
-- KEYS[3] claimed    zset  key -> deadline (unix millis); the lease
-- KEYS[4] fence      hash  key -> monotonic claim counter
-- KEYS[5] attempts   hash  message id -> how many times it has been delivered
-- ARGV[1] prefix     key namespace, e.g. {q:orders} — see the note on cluster hash tags
-- ARGV[2] ttl        millis
-- ARGV[3] batch      the most messages to hand over; at least 1
-- returns            {key, token, deadline, backlog, ids, messages, attempts}, or nil when nothing is
--                    claimable
--
-- '''The whole claim is one call.''' The key leaves `ready` and the lease is written in the same script, so
-- there is no instant in which a key is out of the queue and not yet claimed — and therefore nothing to
-- recover, no per-connection holding list, and no liveness to keep for one. That is the difference between
-- this and a blocking `BLMOVE` followed by a second call: the blocking version cannot run logic before the
-- key has already moved.
--
-- `msgs`, `payloads` and `owned` are built here rather than declared in KEYS, because which key is claimed
-- is not known until the pop. That is only safe because every name shares the `prefix` hash tag and
-- therefore one cluster slot.
local ready, state, claimed, fence, attempts = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local prefix, ttl, batch = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])

-- The head, oldest first: RPUSH on the way in and LPOP here is FIFO across keys, so a key that has waited
-- longest is served first.
local key = redis.call('LPOP', ready)

if not key then
  return nil
end

local msgs     = prefix .. ':msgs:' .. key
local payloads = prefix .. ':payloads:' .. key
local owned    = prefix .. ':owned:' .. key

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

return { key, redis.call('HINCRBY', fence, key, 1), now + ttl, backlog, ids, messages, counts }
