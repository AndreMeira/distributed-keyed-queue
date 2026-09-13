-- Remove holds abandoned past the grace window, and wake any waiter on each freed lock.
--
-- KEYS[1] held, KEYS[2] tokens, KEYS[3] wake (a stream), KEYS[4] waiting  (see acquire.lua)
-- ARGV[1] grace   millis past lease expiry a hold survives before it may be removed
-- ARGV[2] limit   the most holds one pass removes, and the most dead waiter lists it deletes
-- ARGV[3] prefix  what a lock's waiters-list key starts with; the name completes it
-- returns the names removed, oldest lease first; empty when nothing is abandoned
--
-- '''The grace window is the contract, not an implementation detail.''' Refresh is token-only — a holder
-- whose lease lapsed but whom nobody displaced is late, not lost, and may still extend. A trim is a
-- displacement by nobody, so it must only take holds no live holder can plausibly still mean to extend:
-- expired for longer than the grace, not merely expired.
--
-- The wake follows the release script's rule: appended here, in the same script that frees the lock, so it
-- cannot precede the state it announces.
local held, tokens, wake, waiting = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local grace, limit, prefix = tonumber(ARGV[1]), tonumber(ARGV[2]), ARGV[3]

local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local names = redis.call('ZRANGEBYSCORE', held, '-inf', now - grace, 'LIMIT', 0, limit)
for _, name in ipairs(names) do
  redis.call('HDEL', tokens, name)
  redis.call('ZREM', held, name)
  redis.call('XADD', wake, 'MAXLEN', '~', 1000, '*', 'kind', 'l', 'name', name)
end
-- A waiters list whose latest ticket deadline is past the grace holds only expired tickets: every waiter
-- is long gone, and no acquire will visit the name to prune it. Building the key from the prefix is legal
-- for the same reason claim.lua builds per-key names: everything shares the tag, and so the slot.
for _, name in ipairs(redis.call('ZRANGEBYSCORE', waiting, '-inf', now - grace, 'LIMIT', 0, limit)) do
  redis.call('DEL', prefix .. name)
  redis.call('ZREM', waiting, name)
end

return names
