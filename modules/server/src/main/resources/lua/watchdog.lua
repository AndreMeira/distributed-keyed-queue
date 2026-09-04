-- Two sweeps: claims whose consumer went silent, and keys held back after a failure whose backoff has
-- elapsed.
--
-- KEYS[1] claimed    zset  key -> deadline
-- KEYS[2] state      hash
-- KEYS[3] ready      list
-- KEYS[4] fence      hash
-- KEYS[5] delayed    zset  key -> when it may be worked again
-- KEYS[6] wake       stream  one entry per key made claimable
-- ARGV[1] limit      most entries to handle per sweep
-- ARGV[2] prefix     key namespace, e.g. {q:orders} — see the note on cluster hash tags
-- returns            {reclaimed keys, released keys}
--
-- Idempotent, so every pod can run it and no leader election is needed. Bounded by `limit` because a script
-- blocks the whole server — whatever is left over is picked up by the next sweep.
--
-- '''One kind of death, not two.''' A claim is granted in a single call, so a key is either in `ready` or
-- claimed — there is no half-claimed state to recover and no per-connection liveness to track. The lease is
-- the only thing that expires.
--
-- `owned` is built here rather than declared in KEYS, because which keys have expired is not known until
-- the range query runs. That is only safe because every name shares the `prefix` hash tag and therefore one
-- cluster slot.
local claimed, state, ready, fence, delayed, wake =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
local limit, prefix = tonumber(ARGV[1]), ARGV[2]

local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

-- (1) A consumer went silent while holding a claim: the key is in no list, so `claimed` is the only record.
-- Its messages never left `msgs`, so all that is revoked here is the ownership.
local expired = redis.call('ZRANGEBYSCORE', claimed, '-inf', now, 'LIMIT', 0, limit)

for _, key in ipairs(expired) do
  -- Nothing to move. A claim owns messages without taking them out of the key's list, so recovering one is
  -- a matter of forgetting the ownership — the messages are already where they belong, in producer order.
  -- The attempt counts are left alone on purpose: a reclaim IS a delivery that did not finish, and it
  -- should show.
  redis.call('DEL', prefix .. ':owned:' .. key)

  -- Revoking the claim invalidates the token, so the silent consumer's settle is rejected even if nobody
  -- has claimed the key yet. Without this, a zombie finishing late would re-queue an already-queued key.
  redis.call('HINCRBY', fence, key, 1)

  redis.call('ZREM', claimed, key)
  redis.call('HSET', state, key, 'queued')

  -- Not if a nack asked the key to wait. A claim may be settled piece by piece, so a nack can set a backoff
  -- and leave the claim alive — and if that claim then expires, pushing here as well would put the key on
  -- `ready` twice: once now, once when the due sweep finds the backoff elapsed. Two entries mean two
  -- claimers, and although the fence stops the loser corrupting anything, it does the work for nothing.
  if redis.call('ZSCORE', delayed, key) == false then
    redis.call('RPUSH', ready, key)
    redis.call('XADD', wake, 'MAXLEN', '~', 1000, '*', 'key', key)
  end
end

-- (2) A failed message asked to be retried later. Its key was left `queued` but off `ready`, so nothing
-- could pick it up and no producer would re-push it. This is what puts it back when its backoff elapses.
local due = redis.call('ZRANGEBYSCORE', delayed, '-inf', now, 'LIMIT', 0, limit)

for _, key in ipairs(due) do
  redis.call('ZREM', delayed, key)
  redis.call('RPUSH', ready, key)
  redis.call('XADD', wake, 'MAXLEN', '~', 1000, '*', 'key', key)
end

return { expired, due }
