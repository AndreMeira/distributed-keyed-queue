-- Settle the in-flight message and decide what happens to its key next.
--
-- KEYS[1] state      hash
-- KEYS[2] claimed    zset
-- KEYS[3] fence      hash
-- KEYS[4] msgs       list
-- KEYS[5] inflight   list
-- KEYS[6] ready      list
-- KEYS[7] attempts   hash
-- KEYS[8] delayed    zset  key -> when it may be worked again (unix millis)
-- ARGV[1] key
-- ARGV[2] token      the token handed out by consume.lua
-- ARGV[3] outcome    'done' | 'failed'
-- ARGV[4] retryAfter millis; 'failed' only, 0 means immediately
-- ARGV[5] discard    how many to drop from the head after settling; 'done' only, 0 means none
-- returns            1 when applied, 0 when the claim was stale
--
-- The token check is not an optimisation. A missed heartbeat only means the worker cannot be heard, so the
-- watchdog may already have revoked this claim and given the key to somebody else. Without the guard, a
-- zombie would re-queue a key that is already queued — two workers on one key, which is exactly the
-- invariant everything else here protects.
local state, claimed, fence, msgs, inflight, ready, attempts, delayed =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7], KEYS[8]
local key, token, outcome, retryAfter = ARGV[1], tonumber(ARGV[2]), ARGV[3], tonumber(ARGV[4])
local discard = tonumber(ARGV[5]) or 0

if tonumber(redis.call('HGET', fence, key) or 0) ~= token then
  return 0
end

-- Completing ends the claim, so it must end the token too: a token authorises exactly ONE transition.
-- Without this, a retried settle — an at-least-once RPC, a client resending after a timeout — would apply
-- twice and push the key onto `ready` twice, which is two workers on one key.
redis.call('HINCRBY', fence, key, 1)
redis.call('ZREM', claimed, key)

if outcome == 'done' then
  redis.call('DEL', inflight)
  redis.call('HDEL', attempts, key)   -- the head moves on, so its delivery count starts again

  -- Conflation: the caller looked at `backlog_depth` on its delivery, decided those messages were
  -- superseded, and asked for them to go. Safe here and nowhere else, because the key is still held.
  --
  -- BEFORE the emptiness check below, which is what decides whether this key goes back on `ready` or
  -- becomes idle. Discarding after it would leave a key queued as ready with nothing behind it, and no
  -- sweep would repair that: it is not a lapsed claim, not a dead worker, and not an elapsed backoff.
  --
  -- LTRIM clamps, so a discard larger than what remains simply empties the key. That is not an error: the
  -- caller asked for everything behind it to go, and everything behind it went.
  if discard > 0 then
    redis.call('LTRIM', msgs, discard, -1)
  end
else
  -- Back to the HEAD: a retry must not reorder the key it belongs to, and the attempt count stays with it.
  redis.call('LMOVE', inflight, msgs, 'RIGHT', 'LEFT')
end

if redis.call('LLEN', msgs) == 0 then
  redis.call('HDEL', state, key)      -- idle is the absence of an entry
  return 1
end

redis.call('HSET', state, key, 'queued')

if outcome ~= 'done' and retryAfter > 0 then
  -- Held back rather than ready: the key stays `queued`, so a producer will not push it either, and the
  -- watchdog's due-sweep is what puts it back. That is how a failing message backs off without spinning.
  local now = redis.call('TIME')
  now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
  redis.call('ZADD', delayed, now + retryAfter, key)
else
  redis.call('RPUSH', ready, key)
end

return 1
