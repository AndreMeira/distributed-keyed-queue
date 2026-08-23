-- Two sweeps: claims whose worker went silent, and workers that died before their claim was granted.
--
-- KEYS[1] claimed    zset  key -> deadline
-- KEYS[2] state      hash
-- KEYS[3] ready      list
-- KEYS[4] fence      hash
-- KEYS[5] workers    zset  worker -> deadline
-- ARGV[1] now        unix millis
-- ARGV[2] limit      most entries to handle per sweep
-- ARGV[3] prefix     key namespace, e.g. {q:orders} — see the note on cluster hash tags
-- returns            {reclaimed keys, recovered workers}
--
-- Idempotent, so every pod can run it and no leader election is needed. Bounded by `limit` because a script
-- blocks the whole server — whatever is left over is picked up by the next sweep.
--
-- `msgs`, `inflight` and `claiming` are built here rather than declared in KEYS, because which keys and
-- workers have expired is not known until the range queries run. That is only safe because every name shares
-- the `prefix` hash tag and therefore one cluster slot.
local claimed, state, ready, fence, workers = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local now, limit, prefix = tonumber(ARGV[1]), tonumber(ARGV[2]), ARGV[3]

-- (1) A worker went silent while holding a claim: the key is in no list, so `claimed` is the only record.
local expired = redis.call('ZRANGEBYSCORE', claimed, '-inf', now, 'LIMIT', 0, limit)

for _, key in ipairs(expired) do
  -- Back to the HEAD of the key's FIFO: a retry must not reorder the key it belongs to.
  redis.call('LMOVE', prefix .. ':inflight:' .. key, prefix .. ':msgs:' .. key, 'RIGHT', 'LEFT')

  -- Revoking the claim invalidates the token, so the silent worker's completion is rejected even if nobody
  -- has claimed the key yet. Without this, a zombie finishing late would re-queue an already-queued key.
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

return { expired, dead }
