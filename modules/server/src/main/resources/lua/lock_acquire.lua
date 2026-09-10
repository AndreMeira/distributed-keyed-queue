-- Take a named lock if it is free, reclaiming it inline when the current lease has expired.
--
-- KEYS[1] held    zset  lockName -> lease deadline (unix millis); a live entry means held
-- KEYS[2] fence   hash  lockName -> monotonic generation; the fencing token
-- ARGV[1] name
-- ARGV[2] ttl     millis
-- returns {token, leaseUntil}, or nil when it is held under a live lease
--
-- '''No sweep needed, because acquire is named.''' Unlike the queue's claim, which pops the oldest key and
-- so cannot ask "is THIS key's lease expired", a lock acquire names its resource and checks it directly.
-- An expired lease is reclaimed here, on demand, by whoever next wants the lock — so a dead holder needs
-- no background pass to release it, only a contender. A lock holds no work, so a lock nobody is waiting for
-- does not need reclaiming at all.
local held, fence = KEYS[1], KEYS[2]
local name, ttl = ARGV[1], tonumber(ARGV[2])

local now = redis.call('TIME')
now = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)

if tonumber(redis.call('ZSCORE', held, name) or 0) > now then
  return nil   -- held under a live lease
end

-- free, or expired: take it. The fence advances on every grant, so a prior holder's token is now stale.
local token = redis.call('HINCRBY', fence, name, 1)
redis.call('ZADD', held, now + ttl, name)
return { token, now + ttl }
