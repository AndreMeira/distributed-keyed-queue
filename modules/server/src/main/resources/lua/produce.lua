-- Append a message to its key's FIFO, and make the key ready if nothing is working it.
--
-- KEYS[1] ready      list  keys ready to be claimed
-- KEYS[2] state      hash  key -> queued | processing   (absent == idle, so the hash stays small)
-- KEYS[3] msgs       list  this key's message IDS, oldest first
-- KEYS[4] payloads   hash  message id -> the message itself
-- KEYS[5] wake       stream  one entry per key made claimable
-- ARGV[1] key
-- ARGV[2] id         the message's id, unique among this key's queued messages
-- ARGV[3] payload
-- returns            the key's queue depth after the append
--
-- The state guard is the whole point: a key already `queued` is in `ready` (or held back in `delayed`) and
-- must not be pushed twice, and a key `processing` is re-queued by whoever finishes it, or by the watchdog.
-- Two consumers on one key is the one thing this design must never allow.
--
-- '''Ids in the list, messages in a hash.''' The list is what carries order; the hash is what carries cargo.
-- Splitting them is what lets everything else address a message by name — discard this one, look at the next
-- three — without Redis having to read inside a payload it cannot parse.
local ready, state, msgs, payloads, wake = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local key, id, payload = ARGV[1], ARGV[2], ARGV[3]

-- HSETNX rather than HSET, and the whole append hangs off it: an id already queued for this key is the same
-- message arriving twice — a producer retrying an at-least-once send — and enqueuing it again would deliver
-- it twice. Accepted silently rather than refused, because from the producer's side the message *is* queued,
-- which is what it asked for.
--
-- The dedupe window is the key's current backlog, not all time: the id leaves this hash when the message is
-- settled, so the same id may be sent again afterwards. Anything wider would need a record of every id ever
-- seen, and a policy for when to forget it.
if redis.call('HSETNX', payloads, id, payload) == 1 then
  redis.call('RPUSH', msgs, id)

  if not redis.call('HGET', state, key) then
    redis.call('HSET', state, key, 'queued')
    redis.call('RPUSH', ready, key)
    -- In the same call that made it claimable: a consumer woken by this entry cannot arrive before the
    -- work it announces, and a crash cannot land between the two.
    redis.call('XADD', wake, 'MAXLEN', '~', 1000, '*', 'key', key)
  end
end

return redis.call('LLEN', msgs)
