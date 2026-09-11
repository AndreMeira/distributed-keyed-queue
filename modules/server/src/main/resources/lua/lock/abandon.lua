-- Withdraw a ticket: a waiter leaving on its own patience takes its place in the queue with it.
--
-- KEYS[1] waiting, KEYS[2] waiters  (see acquire.lua)
-- ARGV[1] name, ARGV[2] ticketId
-- returns 1 when withdrawn, 0 when the ticket was already gone
--
-- Best effort — a waiter that dies without this is pruned at its deadline by the next pass over the head.
local waiting, waiters = KEYS[1], KEYS[2]
local name, id = ARGV[1], ARGV[2]

for _, ticket in ipairs(redis.call('LRANGE', waiters, 0, -1)) do
  if string.sub(ticket, 1, string.find(ticket, ':') - 1) == id then
    redis.call('LREM', waiters, 1, ticket)
    if redis.call('LLEN', waiters) == 0 then redis.call('ZREM', waiting, name) end
    return 1
  end
end
return 0
