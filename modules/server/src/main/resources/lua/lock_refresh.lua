-- Push a held lock's lease forward, if the caller still holds it.
--
-- KEYS[1] held, KEYS[2] fence
-- ARGV[1] name, ARGV[2] token, ARGV[3] ttl (millis)
-- returns {leaseUntil, 1} when renewed, {0, 0} when the token is stale
--
-- Token-only, deliberately not lease-expiry: a holder whose lease lapsed but whom nobody has displaced is
-- late, not lost, and may extend. Displacement is a fresh acquire, which advances the fence — so a real
-- loss is a token mismatch, which this reports rather than silently renewing another holder's lock.
local held, fence = KEYS[1], KEYS[2]
local name, token, ttl = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])

if tonumber(redis.call('HGET', fence, name) or 0) ~= token then
  return { 0, 0 }
end

local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
redis.call('ZADD', held, now + ttl, name)
return { now + ttl, 1 }
