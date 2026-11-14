-- Release the keys of one partition whose backoff has elapsed, so they are claimable again.
--
-- KEYS[1] delayed    zset  <len(queue)>:<queue><key> -> when that key may be worked again (unix millis)
-- KEYS[2] wake       stream  the partition's; one entry per queue in which a key was made claimable
-- ARGV[1] limit      the most elapsed backoffs to release in this call
-- ARGV[2] queues     what every queue key of the partition starts with, e.g. {p:3}:v4:q:
-- returns            {released, full (1 or 0), millis until the next backoff ends, or -1 when none}
--
-- A nack that asks for a wait holds back its KEY, not one message (C6): settle.lua scores the key here instead of
-- putting it back in `ready`, and enqueue.lua and move.lua will not add it either while it is here. This is what
-- puts it back, on the tick the backoff mover asks for at its end.
--
-- Idempotent, so every instance can run it at the same moment: one releases a key and the others find it gone.
-- Bounded by `limit` because a script blocks the whole server; a full call says so, and the caller repeats it.
--
-- '''The member names the queue as well as the key.''' One `delayed` per partition, shared by its queues, so a
-- pass is one script per partition whatever the number of queues, and no instance needs a list of queues to know
-- what to look at. `<len(queue)>:<queue><key>`, length-prefixed so no character of a queue name can pass for a
-- separator; the key takes what is left. Lua's `#` counts bytes, which makes the length right for any UTF-8.
--
-- '''One wake per queue, not per key''', as in move.lua: the readiness is per queue, and a burst of backoffs
-- ending together must not trim unread entries from the stream.
--
-- Every queue key is built here from `queues`, which is legal because the partition's tag puts them all in one
-- slot.
local delayed, wake = KEYS[1], KEYS[2]
local limit, queues = tonumber(ARGV[1]), ARGV[2]

local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local due = redis.call('ZRANGEBYSCORE', delayed, '-inf', now, 'LIMIT', 0, limit)

local released = 0
local woken = {}

for _, member in ipairs(due) do
  redis.call('ZREM', delayed, member)
  released = released + 1

  local colon = string.find(member, ':', 1, true)
  local queueLength = tonumber(string.sub(member, 1, colon - 1))
  local queue = string.sub(member, colon + 1, colon + queueLength)
  local key = string.sub(member, colon + queueLength + 1)
  local prefix = queues .. queue

  -- Not while a claim still holds the key. A partial nack sets the backoff and leaves the claim alive, and the
  -- backoff may end while the holder is still working the rest of its batch: publishing the key then would let a
  -- second claim take it from under the first. With the backoff gone, whoever ends the claim puts the key back,
  -- settle.lua at its end or the reclaim in sweep.lua, since both push when `delayed` holds nothing for it.
  if redis.call('ZSCORE', prefix .. ':claimed', key) == false then
    redis.call('ZADD', prefix .. ':ready', redis.call('INCR', prefix .. ':seq'), key)
    woken[queue] = true
  end
end

for queue in pairs(woken) do
  redis.call('XADD', wake, 'MAXLEN', '~', 1000, '*', 'kind', 'q', 'name', queue)
end

local full = 0
if #due >= limit then full = 1 end

local nextIn = -1
local first = redis.call('ZRANGE', delayed, 0, 0, 'WITHSCORES')
if first[2] then nextIn = math.max(tonumber(first[2]) - now, 0) end

return { released, full, nextIn }
