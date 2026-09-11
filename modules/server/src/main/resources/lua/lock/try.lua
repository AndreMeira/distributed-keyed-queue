-- Take a named lock only if it is free now and nobody queued first; reclaims an expired lease inline.
--
-- KEYS[1] held, KEYS[2] tokens, KEYS[3] fence, KEYS[4] waiting, KEYS[5] waiters  (see acquire.lua)
-- ARGV[1] name, ARGV[2] ttl (millis)
-- returns {token, leaseUntil}, or nil when held under a live lease or queued for
--
-- '''"Free now" includes free of waiters.''' A live ticket means someone queued first, and granting past
-- them would be the barging the tickets exist to end — so a try refuses what an enter would queue behind.
local held, tokens, fence, waiting, waiters = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local name, ttl = ARGV[1], tonumber(ARGV[2])

local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local head = redis.call('LINDEX', waiters, 0)
while head do
  local deadline = tonumber(string.sub(head, string.find(head, ':') + 1))
  if deadline > now then break end
  redis.call('LPOP', waiters)
  head = redis.call('LINDEX', waiters, 0)
end
if not head then redis.call('ZREM', waiting, name) end

if head or tonumber(redis.call('ZSCORE', held, name) or 0) > now then
  return nil
end

local token = redis.call('INCR', fence)
redis.call('HSET', tokens, name, token)
redis.call('ZADD', held, now + ttl, name)
return { token, now + ttl }
