-- Append a message to its key's FIFO, and make the key ready if nothing is working it.
--
-- KEYS[1] ready      zset  key -> when it became claimable (unix millis)
-- KEYS[2] claimed    zset  key -> lease deadline; a key in here is being worked
-- KEYS[3] delayed    zset  key -> when it may be worked again; a key in here is waiting out a backoff
-- KEYS[4] msgs       list  this key's message IDS, oldest first
-- KEYS[5] payloads   hash  message id -> the message itself
-- KEYS[6] wake       stream  one entry per key made claimable
-- KEYS[7] sequence   string  the counter that scores `ready`; arrival order, not a clock
-- ARGV[1] key
-- ARGV[2] id         the message's id, unique among this key's queued messages
-- ARGV[3] payload
-- ARGV[4] queue      named in the wake entry, because the stream is shared by the whole bucket
-- returns            the key's queue depth after the append
--
-- '''A key must be accounted for exactly once.''' It is accounted for when it is in `ready`, or held by a
-- claim, or waiting out a backoff in `delayed` — and in the last two it is deliberately absent from `ready`,
-- to be put back by whoever finishes it or by the watchdog. Adding it here as well would put it in `ready`
-- alongside a live claim, and two consumers on one key is the one thing this design must never allow.
--
-- The three checks are the whole guard, and being in one script is what makes them atomic. A key already in
-- `ready` keeps the score it has: it must not lose its place in line because another message arrived for it.
--
-- '''Ids in the list, messages in a hash.''' The list is what carries order; the hash is what carries cargo.
-- Splitting them is what lets everything else address a message by name — discard this one, look at the next
-- three — without Redis having to read inside a payload it cannot parse.
local ready, claimed, delayed, msgs, payloads, wake, sequence =
  KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7]
local key, id, payload, queue = ARGV[1], ARGV[2], ARGV[3], ARGV[4]

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

  if redis.call('ZSCORE', ready, key) == false
    and redis.call('ZSCORE', claimed, key) == false
    and redis.call('ZSCORE', delayed, key) == false then
    redis.call('ZADD', ready, redis.call('INCR', sequence), key)

    -- Only on the transition, and in the same call that made it claimable: a consumer woken by this entry
    -- cannot arrive before the work it announces, and a crash cannot land between the two.
    redis.call('XADD', wake, 'MAXLEN', '~', 1000, '*', 'queue', queue, 'key', key)
  end
end

return redis.call('LLEN', msgs)
