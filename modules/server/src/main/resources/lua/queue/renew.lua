-- Push the deadline forward on every claim a consumer still holds.
--
-- KEYS[1] claimed    zset  key -> deadline
-- KEYS[2] fence      hash  key -> claim counter
-- ARGV[1] ttl        millis
-- ARGV[2..] key, token, key, token, ...
-- returns            {renewedUntil, staleKeys}
--
-- A heartbeat is a *renewal*, which is why a long-running handler needs no lease long enough to cover it.
-- Two guards keep a confused consumer honest: `XX` never resurrects a claim the watchdog already revoked,
-- and the token check stops one extending a claim that has since been handed to somebody else — where a
-- blind `XX` would happily push the new owner's deadline around.
--
-- A claim is stale when the fence names a different owner, or when the key is no longer in `claimed`.
-- `ZSCORE` is what answers the second: it is asked before the write, so the answer is about whether the
-- claim is still there and not about whether the deadline moved. A renewal that lands in the same
-- millisecond as the one before it writes the deadline it already had, and is still a renewal.
--
-- It returns the keys it could NOT renew rather than a count: the caller has to stop working those, and a
-- number does not say which.
local claimed, fence = KEYS[1], KEYS[2]
local ttl = tonumber(ARGV[1])

local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

local stale = {}
for i = 2, #ARGV, 2 do
  local key, token = ARGV[i], tonumber(ARGV[i + 1])
  local owned = tonumber(redis.call('HGET', fence, key) or 0) == token
  if owned and redis.call('ZSCORE', claimed, key) then
    redis.call('ZADD', claimed, 'XX', now + ttl, key)
  else
    table.insert(stale, key)
  end
end

return { now + ttl, stale }
