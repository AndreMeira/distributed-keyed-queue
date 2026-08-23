-- Append a message to its key's FIFO, and make the key ready if nothing is working it.
--
-- KEYS[1] ready      list  keys ready to be claimed
-- KEYS[2] state      hash  key -> queued | processing   (absent == idle, so the hash stays small)
-- KEYS[3] msgs       list  this key's messages, oldest first
-- ARGV[1] key
-- ARGV[2] payload
-- returns            the key's queue depth after the append
--
-- The state guard is the whole point: a key already `queued` is in `ready` once and must not be
-- pushed twice, and a key `processing` is re-queued by whoever finishes it (complete.lua) or by
-- the watchdog. Two workers on one key is the one thing this design must never allow.
local ready, state, msgs = KEYS[1], KEYS[2], KEYS[3]
local key, payload = ARGV[1], ARGV[2]

redis.call('RPUSH', msgs, payload)

if not redis.call('HGET', state, key) then
  redis.call('HSET', state, key, 'queued')
  redis.call('RPUSH', ready, key)
end

return redis.call('LLEN', msgs)
