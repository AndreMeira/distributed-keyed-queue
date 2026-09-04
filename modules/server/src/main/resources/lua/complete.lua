-- Settle some of what a claim owns, and decide what happens to its key when nothing is left owed.
--
-- KEYS[1] state      hash
-- KEYS[2] claimed    zset  key -> lease deadline
-- KEYS[3] fence      hash  key -> monotonic claim counter
-- KEYS[4] msgs       list  this key's message ids, producer order, until acknowledged
-- KEYS[5] payloads   hash  message id -> the message itself
-- KEYS[6] owned      set   the ids this claim owns and has not settled
-- KEYS[7] attempts   hash  message id -> delivery count
-- KEYS[8] ready      list
-- KEYS[9] delayed    zset  key -> when it may be worked again (unix millis)
-- KEYS[10] wake      stream  one entry per key made claimable
-- ARGV[1] key
-- ARGV[2] token      the token handed out by consume.lua
-- ARGV[3] retryAfter millis; applied when a nack asks to be held back, 0 for immediately
-- ARGV[4..] id, verdict, id, verdict, ...  verdict is 'ack' | 'nack'
-- returns            1 when applied, 0 when the claim was stale
--
-- The token check is not an optimisation. A missed heartbeat only means the worker cannot be heard, so the
-- watchdog may already have revoked this claim and given the key to somebody else. Without the guard, a
-- zombie would settle messages the new owner is working.
--
-- '''The token is checked every time and advanced only once.''' A claim may be settled piece by piece, so a
-- token has to stay valid across several calls; what stops a settle applying twice is that settling removes
-- the id from `owned`, and removing it again finds nothing. The counter moves when the claim ENDS, which is
-- what invalidates a zombie still holding it.
local state, claimed, fence, msgs, payloads, owned, attempts, ready, delayed, wake =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7], KEYS[8], KEYS[9], KEYS[10]
local key, token, retryAfter = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])

if tonumber(redis.call('HGET', fence, key) or 0) ~= token then
  return 0
end

for i = 4, #ARGV, 2 do
  local id, verdict = ARGV[i], ARGV[i + 1]

  -- Only what this claim owns, and only once: SREM answers both questions at once. An id it was never given
  -- — another key's, or one it invented — removes nothing and is silently ignored, which is also what makes
  -- a retried settle harmless.
  if redis.call('SREM', owned, id) == 1 then
    if verdict == 'ack' then
      -- Acknowledged means gone: out of the order, and its payload and delivery count with it.
      redis.call('LREM', msgs, 0, id)
      redis.call('HDEL', payloads, id)
      redis.call('HDEL', attempts, id)
    elseif retryAfter > 0 then
      -- A nack leaves the message exactly where it is — still in `msgs`, still in producer order — so there
      -- is nothing to put back and nothing to reorder. All it may ask for is that the KEY waits before
      -- anyone works it again. GT so that several nacks in one claim leave the longest wait standing.
      local now = redis.call('TIME')
      now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
      redis.call('ZADD', delayed, 'GT', now + retryAfter, key)
    end
  end
end

if redis.call('SCARD', owned) > 0 then
  return 1   -- still owed something; the claim, and its token, live on
end

-- Nothing left owed, so the claim is over. Advancing the fence here and nowhere else is what makes the
-- token good for the whole claim and worthless afterwards.
redis.call('HINCRBY', fence, key, 1)
redis.call('ZREM', claimed, key)

if redis.call('LLEN', msgs) == 0 then
  redis.call('HDEL', state, key)      -- idle is the absence of an entry
  return 1
end

redis.call('HSET', state, key, 'queued')

-- Held back rather than ready when a nack asked for it: the key stays `queued`, so a producer will not push
-- it either, and the watchdog's due-sweep is what puts it back. That is how a failing message backs off
-- without spinning.
if redis.call('ZSCORE', delayed, key) == false then
  redis.call('RPUSH', ready, key)
  redis.call('XADD', wake, 'MAXLEN', '~', 1000, '*', 'key', key)
end

return 1
