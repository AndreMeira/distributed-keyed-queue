-- Renew this worker's liveness, and push the deadline forward on every claim it still holds.
--
-- KEYS[1] claimed    zset  key -> deadline
-- KEYS[2] fence      hash  key -> claim counter
-- KEYS[3] workers    zset  worker -> deadline
-- ARGV[1] now        unix millis
-- ARGV[2] ttl        millis
-- ARGV[3] worker     this worker's id
-- ARGV[4..] key, token, key, token, ...
-- returns            how many claims were renewed
--
-- A heartbeat is a *renewal*, which is why a long-running handler needs no lease long enough to cover it.
-- Two guards keep a confused worker honest: `XX` never resurrects a claim the watchdog already revoked, and
-- the token check stops a worker extending a claim that has since been handed to somebody else — where a
-- blind `XX` would happily push the new owner's deadline around.
--
-- The worker's own entry is written WITHOUT `XX`: this call is also how a worker registers itself, and it
-- must do so *before its first BLMOVE*. A worker that claims before it is known cannot be recovered — its
-- claiming list would have no liveness entry to expire (see watchdog.lua).
local claimed, fence, workers = KEYS[1], KEYS[2], KEYS[3]
local now, ttl, worker = tonumber(ARGV[1]), tonumber(ARGV[2]), ARGV[3]

redis.call('ZADD', workers, now + ttl, worker)

local renewed = 0
for i = 4, #ARGV, 2 do
  local key, token = ARGV[i], tonumber(ARGV[i + 1])
  if tonumber(redis.call('HGET', fence, key) or 0) == token then
    renewed = renewed + redis.call('ZADD', claimed, 'XX', 'CH', now + ttl, key)
  end
end

return renewed
