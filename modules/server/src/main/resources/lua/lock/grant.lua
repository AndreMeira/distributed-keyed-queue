-- One ticketed waiter asking for its turn: granted when the lock is free and its ticket is head.
--
-- KEYS[1] held, KEYS[2] tokens, KEYS[3] fence, KEYS[4] waiting, KEYS[5] waiters  (see acquire.lua)
-- ARGV[1] name, ARGV[2] ticketId, ARGV[3] ttl (millis)
-- returns {1, token, leaseUntil} granted | {0, recheckMillis} not yet | {2, 0} ticket gone
--
-- Every waiter runs this off the same broadcast wake; only the head can win, so the race is a check.
-- A refusal names the delay after which the answer can change — the lease's end when the lock is held,
-- the head ticket's deadline when queued behind it — so a waiter parks until a known event, not on a poll.
local held, tokens, fence, waiting, waiters = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local name, id, ttl = ARGV[1], ARGV[2], tonumber(ARGV[3])

local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local head = redis.call('LINDEX', waiters, 0)
while head do
  local deadline = tonumber(string.sub(head, string.find(head, ':') + 1))
  if deadline > now then break end
  redis.call('LPOP', waiters)
  head = redis.call('LINDEX', waiters, 0)
end
if not head then
  redis.call('ZREM', waiting, name)
  return { 2, 0 }
end

local lease = tonumber(redis.call('ZSCORE', held, name) or 0)

if string.sub(head, 1, string.find(head, ':') - 1) == id then
  if lease > now then return { 0, lease - now } end
  redis.call('LPOP', waiters)
  if redis.call('LLEN', waiters) == 0 then redis.call('ZREM', waiting, name) end
  local token = redis.call('INCR', fence)
  redis.call('HSET', tokens, name, token)
  redis.call('ZADD', held, now + ttl, name)
  return { 1, token, now + ttl }
end

-- Behind the head: still queued at all?
local mine = nil
for _, ticket in ipairs(redis.call('LRANGE', waiters, 0, -1)) do
  if string.sub(ticket, 1, string.find(ticket, ':') - 1) == id then mine = ticket break end
end
if not mine then return { 2, 0 } end

local recheck
if lease > now then recheck = lease - now
else recheck = tonumber(string.sub(head, string.find(head, ':') + 1)) - now
end
return { 0, recheck }
