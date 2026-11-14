-- Record a message to be appended to its key's line once its delay has elapsed.
--
-- KEYS[1] scheduled  zset  member -> when the message falls due (unix millis, the store's clock)
-- KEYS[2] payloads   hash  member -> the message, until it is moved
-- ARGV[1] queue
-- ARGV[2] key
-- ARGV[3] id         the message's id
-- ARGV[4] payload    the message as stored
-- ARGV[5] delay      millis from now
-- returns            {1, delay} when recorded, {0, remaining millis of the existing schedule} when ignored
--
-- '''The member is the message's identity.''' It names the queue, the key and the id, each length-prefixed so
-- no character of a name can pass for a separator: `<len(queue)>:<queue><len(key)>:<key><id>`. Lua's `#` counts
-- bytes, which is what makes the lengths right for any UTF-8. Built here rather than by the caller, so only
-- Lua ever writes or reads one.
--
-- '''The first schedule wins.''' A member already in the set is the same message scheduled twice — a retried
-- request, or a caller trying to move its due time, which is not offered — so it is left as it is and the answer
-- says so. An id is not compared with the key's queued messages here: that happens when the message is moved,
-- as for any enqueue.
--
-- The due time is the store's clock plus the delay, so no caller's or instance's clock is ever compared with it.
local scheduled, payloads = KEYS[1], KEYS[2]
local queue, key, id, payload, delay = ARGV[1], ARGV[2], ARGV[3], ARGV[4], tonumber(ARGV[5])

local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local member = #queue .. ':' .. queue .. #key .. ':' .. key .. id

if redis.call('ZADD', scheduled, 'NX', now + delay, member) == 1 then
  redis.call('HSET', payloads, member, payload)
  return { 1, delay }
end

local due = tonumber(redis.call('ZSCORE', scheduled, member))
return { 0, math.max(due - now, 0) }
