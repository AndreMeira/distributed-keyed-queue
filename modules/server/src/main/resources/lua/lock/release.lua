-- Release a lock, if the caller still holds it, and wake a waiter.
--
-- KEYS[1] held, KEYS[2] tokens, KEYS[3] wake  (a stream)
-- ARGV[1] name, ARGV[2] token
-- returns 1 when released, 0 when the token is stale (the lock was reclaimed or already released)
--
-- The token's entry is deleted, so the released token cannot act again — there is nothing left for it to
-- match. A release racing a reclaim loses cleanly: the reclaim overwrote the entry first, so this sees a
-- mismatch and does nothing.
--
-- '''The wake is appended here, only on a real release, in the same script that frees the lock.''' Like the
-- queue's enqueue, the wake cannot precede the state it announces, and a stale release (a mismatch below)
-- returns before reaching it — so no waiter is sent to look at a lock still held.
local held, tokens, wake = KEYS[1], KEYS[2], KEYS[3]
local name, token = ARGV[1], tonumber(ARGV[2])

if tonumber(redis.call('HGET', tokens, name) or 0) ~= token then
  return 0
end

redis.call('HDEL', tokens, name)
redis.call('ZREM', held, name)
-- Field 'queue' names what became free — the same field the queue's wake entries use, so the shared
-- listener reads both with one extractor.
redis.call('XADD', wake, 'MAXLEN', '~', 1000, '*', 'queue', name)
return 1
