-- Enter for a named lock: granted at once when free with nobody queued, a tail ticket otherwise.
--
-- KEYS[1] held     zset    lockName -> lease deadline (unix millis); a live entry means held
-- KEYS[2] tokens   hash    lockName -> the live holder's fence token
-- KEYS[3] fence    string  one counter for every lock; INCR on every grant, and for ticket identity
-- KEYS[4] waiting  zset    lockName -> the latest ticket deadline in its waiters list
-- KEYS[5] waiters  list    this lock's tickets, `id:deadline`, arrival order
-- ARGV[1] name
-- ARGV[2] ttl       millis
-- ARGV[3] patience  millis; the ticket's deadline is now + patience
-- returns {1, token, leaseUntil} granted | {0, ticketId, recheckMillis} queued
--
-- '''Order lives here, in the store.''' Fairness is the waiters list: grants follow ticket order among
-- tickets still within their patience, and a newcomer goes to the tail — so an instance keeps no waiter
-- state and a barger has nothing to barge past the list.
--
-- '''A ticket id is not a fence.''' Both come from the counter, but a ticket is drawn while another fence
-- may be live, so granting with it later could hand out a number below one already seen downstream. The
-- fence is INCRed at grant, always; the ticket id is identity only.
--
-- `recheckMillis` is a delay, not an instant, so the caller never compares the store's clock to its own.
local held, tokens, fence, waiting, waiters = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local name, ttl, patience = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])

local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

-- Drop expired tickets from the head; an abandoned waiter delays nobody past its own patience.
local head = redis.call('LINDEX', waiters, 0)
while head do
  local deadline = tonumber(string.sub(head, string.find(head, ':') + 1))
  if deadline > now then break end
  redis.call('LPOP', waiters)
  head = redis.call('LINDEX', waiters, 0)
end
if not head then redis.call('ZREM', waiting, name) end

local lease = tonumber(redis.call('ZSCORE', held, name) or 0)

if lease <= now and not head then
  local token = redis.call('INCR', fence)
  redis.call('HSET', tokens, name, token)
  redis.call('ZADD', held, now + ttl, name)
  return { 1, token, now + ttl }
end

-- Held, or queued behind someone: take a tail ticket and say when the answer can next change —
-- the lease's end when held, the head ticket's own deadline otherwise.
local id       = redis.call('INCR', fence)
local deadline = now + patience
redis.call('RPUSH', waiters, id .. ':' .. deadline)
redis.call('ZADD', waiting, 'GT', deadline, name)
local recheck
if lease > now then recheck = lease - now
else recheck = tonumber(string.sub(head, string.find(head, ':') + 1)) - now
end
return { 0, id, recheck }
