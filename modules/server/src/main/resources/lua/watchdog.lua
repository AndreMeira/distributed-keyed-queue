-- Three sweeps: claims whose worker went silent, workers that died before their claim was granted, and keys
-- held back after a failure whose backoff has elapsed.
--
-- KEYS[1] claimed    zset  key -> deadline
-- KEYS[2] state      hash
-- KEYS[3] ready      list
-- KEYS[4] fence      hash
-- KEYS[5] workers    zset  worker -> deadline
-- KEYS[6] delayed    zset  key -> when it may be worked again
-- ARGV[1] limit      most entries to handle per sweep
-- ARGV[2] prefix     key namespace, e.g. {q:orders} — see the note on cluster hash tags
-- returns            {reclaimed keys, recovered workers, released keys}
--
-- Idempotent, so every pod can run it and no leader election is needed. Bounded by `limit` because a script
-- blocks the whole server — whatever is left over is picked up by the next sweep.
--
-- `msgs`, `inflight` and `claiming` are built here rather than declared in KEYS, because which keys and
-- workers have expired is not known until the range queries run. That is only safe because every name shares
-- the `prefix` hash tag and therefore one cluster slot.
local claimed, state, ready, fence, workers, delayed =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
local limit, prefix = tonumber(ARGV[1]), ARGV[2]

local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

-- (1) A worker went silent while holding a claim: the key is in no list, so `claimed` is the only record.
local expired = redis.call('ZRANGEBYSCORE', claimed, '-inf', now, 'LIMIT', 0, limit)

for _, key in ipairs(expired) do
  -- Back to the HEAD of the key's FIFO: a retry must not reorder the key it belongs to. The attempt count
  -- is left alone on purpose — a reclaim IS a delivery that did not finish, and it should show.
  redis.call('LMOVE', prefix .. ':inflight:' .. key, prefix .. ':msgs:' .. key, 'RIGHT', 'LEFT')

  -- Revoking the claim invalidates the token, so the silent worker's settle is rejected even if nobody has
  -- claimed the key yet. Without this, a zombie finishing late would re-queue an already-queued key.
  redis.call('HINCRBY', fence, key, 1)

  redis.call('ZREM', claimed, key)
  redis.call('HSET', state, key, 'queued')
  redis.call('RPUSH', ready, key)
end

-- (2) A worker died between its BLMOVE and consume.lua: the key is in that worker's claiming list, still
-- `queued`, with no deadline anywhere. Nothing in sweep (1) can see it — its own claiming list is the only
-- record, which is why the list is per worker and why workers carry a liveness deadline of their own.
local dead = redis.call('ZRANGEBYSCORE', workers, '-inf', now, 'LIMIT', 0, limit)

for _, worker in ipairs(dead) do
  local claiming = prefix .. ':claiming:' .. worker
  -- Tail to head restores the original order at the FRONT of `ready`: these keys were taken before anything
  -- queued behind them, and their `state` is still `queued`, so nothing else needs changing.
  while redis.call('LMOVE', claiming, ready, 'RIGHT', 'LEFT') do end
  redis.call('ZREM', workers, worker)
end

-- (3) A failed message asked to be retried later. Its key was left `queued` but off `ready`, so nothing
-- could pick it up and no producer would re-push it. This is what puts it back when its backoff elapses.
local due = redis.call('ZRANGEBYSCORE', delayed, '-inf', now, 'LIMIT', 0, limit)

for _, key in ipairs(due) do
  redis.call('ZREM', delayed, key)
  redis.call('RPUSH', ready, key)
end

return { expired, dead, due }
