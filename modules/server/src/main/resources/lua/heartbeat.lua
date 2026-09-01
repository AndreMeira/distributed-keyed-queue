-- Renew this worker's liveness, and push the deadline forward on every claim it still holds.
--
-- KEYS[1] claimed    zset  key -> deadline
-- KEYS[2] fence      hash  key -> claim counter
-- KEYS[3] workers    zset  worker -> deadline
-- ARGV[1] ttl        millis
-- ARGV[2] worker     this worker's id, or empty when the caller has no liveness to write
-- ARGV[3..] key, token, key, token, ...
-- returns            {renewedUntil, staleKeys}
--
-- A heartbeat is a *renewal*, which is why a long-running handler needs no lease long enough to cover it.
-- Two guards keep a confused worker honest: `XX` never resurrects a claim the watchdog already revoked, and
-- the token check stops a worker extending a claim that has since been handed to somebody else — where a
-- blind `XX` would happily push the new owner's deadline around.
--
-- The worker's own entry is written WITHOUT `XX`: this call is also how a worker registers itself, and it
-- must do so *before its first BLMOVE*. A worker that claims before it is known cannot be recovered — its
-- claiming list would have no liveness entry to expire (see watchdog.lua).
--
-- It returns the keys it could NOT renew rather than a count: the caller has to stop working those, and a
-- number does not say which.
local claimed, fence, workers = KEYS[1], KEYS[2], KEYS[3]
local ttl, worker = tonumber(ARGV[1]), ARGV[2]

local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

-- Empty when a caller is only renewing claims. A consumer's claims are found by fence token, not by
-- worker, so a renewal has no liveness of its own to write — and writing one would put an entry in
-- `workers` for something that never claims, which is exactly what sweep (2) reads to find abandoned work.
if worker ~= '' then
  redis.call('ZADD', workers, now + ttl, worker)
end

local stale = {}
for i = 3, #ARGV, 2 do
  local key, token = ARGV[i], tonumber(ARGV[i + 1])
  local renewed = 0
  if tonumber(redis.call('HGET', fence, key) or 0) == token then
    renewed = redis.call('ZADD', claimed, 'XX', 'CH', now + ttl, key)
  end
  if renewed == 0 then
    table.insert(stale, key)
  end
end

return { now + ttl, stale }
