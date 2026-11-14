-- Append every scheduled message of one partition that has fallen due to its key's line, as an enqueue would.
--
-- KEYS[1] scheduled  zset  member -> when the message falls due (unix millis, the store's clock)
-- KEYS[2] payloads   hash  member -> the message, until it is moved
-- KEYS[3] wake       stream  the partition's; one entry per queue in which a key was made claimable
-- KEYS[4] delayed    zset  the partition's held-back keys, <len(queue)>:<queue><key> -> when it may be worked
-- ARGV[1] limit      the most due entries to move in this call
-- ARGV[2] queues     what every queue key of the partition starts with, e.g. {p:3}:v3:q:
-- returns            {moved, discarded, full (1 or 0), millis until the next entry falls due, or -1 when none}
--
-- Idempotent, so every instance can run it at the same moment: the script is atomic, so one moves an entry and
-- the others find it gone. Bounded by `limit` because a script blocks the whole server; a full call says so,
-- and the caller repeats it.
--
-- '''Each due message is enqueued, with enqueue's rules.''' It takes its place in its key's line now, behind
-- whatever was sent for the key in between. `HSETNX` on the key's payloads is the same dedupe as an enqueue's:
-- an id still queued for the key is the same message, so the scheduled copy is discarded. A key is made
-- claimable only when it is accounted for nowhere else (`ready`, `claimed`, `delayed`), exactly as in
-- `enqueue.lua`, and scored by its queue's own sequence.
--
-- '''One wake per queue, not per key.''' A wake names its queue, and the readiness is per queue, so one entry
-- per queue touched is enough. A burst of due messages then cannot trim away entries a briefly lagging listener
-- has not read yet: the stream is capped at about 1,000 entries.
--
-- Every queue key is built here from `queues`, the queue's name and the key, rather than declared in KEYS: which
-- queues are due is not known until the range query runs. Legal because the partition's tag puts them all in
-- one slot.
local scheduled, payloads, wake, delayed = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local limit, queues = tonumber(ARGV[1]), ARGV[2]

local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

-- Equal due times come back in member order, so messages of one key due in the same millisecond join its line
-- in id order (S5).
local due = redis.call('ZRANGEBYSCORE', scheduled, '-inf', now, 'LIMIT', 0, limit)

local moved, discarded = 0, 0
local woken = {}

for _, member in ipairs(due) do
  local payload = redis.call('HGET', payloads, member)
  redis.call('ZREM', scheduled, member)
  redis.call('HDEL', payloads, member)

  -- `<len(queue)>:<queue><len(key)>:<key><id>`, read back the way schedule.lua wrote it.
  local colon = string.find(member, ':', 1, true)
  local queueLength = tonumber(string.sub(member, 1, colon - 1))
  local queue = string.sub(member, colon + 1, colon + queueLength)
  local rest = colon + queueLength + 1
  colon = string.find(member, ':', rest, true)
  local keyLength = tonumber(string.sub(member, rest, colon - 1))
  local key = string.sub(member, colon + 1, colon + keyLength)
  local id = string.sub(member, colon + keyLength + 1)

  -- A member without its payload is a half-written entry nothing can deliver; removing it is all there is to do.
  if payload then
    local prefix = queues .. queue
    if redis.call('HSETNX', prefix .. ':payloads:' .. key, id, payload) == 1 then
      redis.call('RPUSH', prefix .. ':msgs:' .. key, id)
      moved = moved + 1

      if redis.call('ZSCORE', prefix .. ':ready', key) == false
        and redis.call('ZSCORE', prefix .. ':claimed', key) == false
        and redis.call('ZSCORE', delayed, #queue .. ':' .. queue .. key) == false then
        redis.call('ZADD', prefix .. ':ready', redis.call('INCR', prefix .. ':seq'), key)
        woken[queue] = true
      end
    else
      discarded = discarded + 1
    end
  end
end

for queue in pairs(woken) do
  redis.call('XADD', wake, 'MAXLEN', '~', 1000, '*', 'kind', 'q', 'name', queue)
end

local full = 0
if #due >= limit then full = 1 end

local nextIn = -1
local first = redis.call('ZRANGE', scheduled, 0, 0, 'WITHSCORES')
if first[2] then nextIn = math.max(tonumber(first[2]) - now, 0) end

return { moved, discarded, full, nextIn }
