---
title: "A keyed queue on Redis — idle pops, per-key exclusivity, and what it costs"
type: research
status: draft
updated: 2026-08-30
tags: [connection, keyed-queue, idle, lease, heartbeat, lua, postgres, exploration]
---

# A keyed queue on Redis

> **Exploration, not a decision.** The commands and pseudo-code below are worked out far enough to expose the
> hazards; none of it is a committed API, and Postgres remains the alternative (§6). What this note is for is
> the *shape* of the thing and the price of each option — naming, module layout and the gRPC surface are all
> still open.
>
> **The built layout has since moved on, and this note is kept as written.** Three things below are no longer
> how it works: `msgs` holds message *ids* with the messages in a `payloads` hash; there is no `inflight`,
> because a claim marks ownership in an `owned` set without moving anything; and a claim covers a *batch* of
> a key's messages, settled one at a time. A nack therefore returns nothing to the head — the message never
> left it — so per-key ordering is now the order messages are *handed out* rather than the order their
> effects land. For what exists, see
> [`../architecture/redis-data-structures.md`](../architecture/redis-data-structures.md); where the two
> disagree, that one is right.

Problem statement and the requirement this serves:
[`research/infrastructure/homelab-message-broker.md`](../../../research/infrastructure/homelab-message-broker.md)
— partition ownership, per-partition order, **per-key serial processing with long-lived handlers**, and
concurrency across keys.

## 1. Why this came up: the blocking dequeue

A client's `Dequeue` is a blocking call with a deadline. If it parks on pod A and a message is enqueued on
pod B, pod A has to learn within milliseconds. On Postgres that means `LISTEN`/`NOTIFY` as a hint plus a poll
floor for correctness, because a notification reaches only sessions listening *at commit time* — no backlog,
no replay — so every reconnect gap is a hole and the hint can never be load-bearing.

Redis makes the wake-up a primitive rather than a mechanism: a blocking pop *is* the cross-node signal, and
the server holding the data is the one that wakes you.

## 2. The shape: the queue holds *keys*, not messages

The move that makes per-key exclusivity structural rather than a consumer-side lock: **`QUEUE` is a list of
keys that are ready and not currently being worked**. A key appears in it at most once, so at most one worker
can hold it — the guarantee falls out of the data structure instead of being enforced by a lock somebody has
to remember to take.

Each key then owns its own FIFO of messages, and a small state machine keeps the two consistent. Seven
structures, each with one job:

**`{q:<queue>}:ready`** — `list` of *keys*, oldest first.
The work queue, and the only place anything blocks: a worker parks on it with `BLMOVE`. It holds keys that
have messages and nobody working them. **Its invariant is the whole design: a key appears at most once**, so
whoever takes it holds it exclusively. Appended to by `produce`, `complete` and `watchdog`; drained by
workers.

**`{q:<queue>}:claiming:<worker>`** — `list` of keys, transient, **one per worker**.
Where a key sits for the instant between `BLMOVE` returning it and `consume.lua` running. It exists only
because Redis scripts cannot block, so a claim has to be two steps.

It is per worker rather than shared for a specific reason. A worker that dies in that seam leaves a key that
is in no other record — `state` still says `queued`, and nothing was written to `claimed`, so the lease sweep
cannot see it. With a shared list there is no way to tell an abandoned entry from one that is mid-transition
right now except by timing, and a timing heuristic that guesses wrong hands one key to two workers. Per
worker, the entry is attributable: it belongs to exactly one worker, whose liveness is a *fact* in `workers`.

**`{q:<queue>}:workers`** — `zset`, worker → deadline in unix millis.
Worker liveness, renewed by the same heartbeat that renews claims. It is what makes the list above
recoverable, and it carries one hard rule: **a worker must register here before its first `BLMOVE`.** One
that claims before it is known has no liveness entry to expire, so nothing can ever recover its claiming
list.

**`{q:<queue>}:claimed`** — `zset`, key → deadline in unix millis.
The lease, and the only thing that makes a dead worker recoverable. Written when a claim is granted, pushed
forward by `heartbeat`, removed by `complete`, and range-queried by `watchdog` — which is why it is a sorted
set and not a hash: the question asked of it is always *"what has expired?"*.

**`{q:<queue>}:state`** — `hash`, key → `queued` | `processing`. **Absence means idle**, which keeps the hash
from growing without bound.
The guard that stops a key being queued twice. It is what lets `produce` decide, in one step, whether a new
message needs to make its key eligible or whether somebody will do that later. It also makes a separate
"waiting" list unnecessary — *waiting* is exactly `processing` with `msgs` non-empty, a fact derived rather
than stored, so it cannot drift.

**`{q:<queue>}:fence`** — `hash`, key → a monotonically increasing integer.
The claim's generation number. Incremented when a claim is granted *and* when one is revoked, so a token
identifies not just "a claim on this key" but *this particular* claim. It is what makes a missed heartbeat
survivable: the silent worker's completion is rejected because its generation is stale.

**`{q:<queue>}:msgs:<key>`** — `list`, that key's messages, oldest at the head.
The per-key FIFO, and the thing whose order the system exists to preserve. Appended at the tail by `produce`,
taken from the head by `consume`, and restored to the head by `watchdog`.

**`{q:<queue>}:inflight:<key>`** — `list` holding at most one message.
The message currently being worked. It exists so a crashed worker's message is recoverable rather than lost:
without it, a message popped from `msgs` lives only in the dead worker's memory. Non-empty implies the key is
`processing` and has a live entry in `claimed`.

**One direction convention, and it is load-bearing: push at the tail, pop at the head.** `RPUSH` to add,
`LEFT` to take — `RPUSH` plus a `RIGHT` pop is a *stack*, and would silently reverse the per-key ordering
this design exists to guarantee. The single exception is the watchdog restoring an in-flight message, which
pushes to the head on purpose. Verified against Valkey 8.1: with `a b c` pushed via `RPUSH`, a `LEFT` pop
returns `a` and a `RIGHT` pop returns `c`.

## 3. The five operations

Each is one Lua script, because each spans several structures and the interleavings are precisely the bugs.
These are the real scripts, reproduced from [`src/main/resources/lua/`](../../src/main/resources/lua/) with
their `KEYS`/`ARGV` headers trimmed — **the files are the source of truth**, and this note can drift from
them. All five were exercised end to end against the compose instance; see §4b for what that run proved.

### `produce` — accept a message, and make its key eligible if nothing is working it

Two effects, one of them conditional. The message always goes to the tail of its key's FIFO. The key is
pushed onto `ready` **only if it is idle** — because a `queued` key is already there and must not appear
twice, and a `processing` key will be re-queued by whoever finishes it, or by the watchdog if that worker
never does. That conditional is the entire per-key exclusivity guarantee, which is why it cannot be two
round trips from the client.

Returns the key's queue depth, which is a metric rather than a decision.

```lua
local ready, state, msgs = KEYS[1], KEYS[2], KEYS[3]
local key, payload = ARGV[1], ARGV[2]

redis.call('RPUSH', msgs, payload)

if not redis.call('HGET', state, key) then
  redis.call('HSET', state, key, 'queued')
  redis.call('RPUSH', ready, key)
end

return redis.call('LLEN', msgs)
```

### `consume` — turn possession of a key into a claim

`BLMOVE` has already moved the key out of `ready`, which means *nobody else can touch it*. That is a lock,
not yet a claim. This script makes it one, indivisibly: take the head message into `inflight`, mark the key
`processing`, register a deadline in `claimed`, and mint a fence token.

The four have to happen together. A crash between taking the message and registering the deadline would
leave the message out of the FIFO with no lease to expire — stalled with nothing to notice it. A deadline
without a token would leave `complete` unable to tell a live claim from a revoked one.

If the key turns out to have no messages, it is released rather than held, and the script returns nil — the
caller claims again within its remaining deadline (§4c).

The blocking part is outside on purpose: Redis scripts cannot block. `claiming` is the seam that leaves,
and it is swept by the watchdog.

**Where the key actually goes.** `BLMOVE ready claiming:<worker> LEFT RIGHT` takes the key off the head of
`ready` and parks it on the tail of *that worker's* claiming list; `consume.lua` then removes it with `LREM`. So once the
claim is granted, **the key is in no list at all** — which is the part that reads as a gap. Its location is
`state` plus `claimed`: those two *are* where a held key lives. Meanwhile the *message* makes its own move,
from the key's FIFO into `inflight`.

Traced through the example from §4b (`k1` holding `m1, m2`, with `k2` also ready):

```
                 before            after BLMOVE          after consume.lua
ready            [k1, k2]          [k2]                  [k2]
claiming:w1      []                [k1]                  []
state            {k1: queued,      {k1: queued,          {k1: processing,
                  k2: queued}       k2: queued}           k2: queued}
claimed          {}                {}                    {k1: now+ttl}
fence            {}                {}                    {k1: 1}
msgs:k1          [m1, m2]          [m1, m2]              [m2]
inflight:k1      []                []                    [m1]
```

Read the last column downwards: `k1` is absent from every list, and the only records that it is being worked
are `state` and `claimed`. That is exactly why losing either one strands the key — no list holds it to fall
back on.

The middle column is the dangerous one. A worker that dies *there* leaves `k1` in `claiming:w1` with no
deadline anywhere, which is why the watchdog needs a second sweep keyed on worker liveness rather than on
`claimed` (§3, `watchdog`).

```lua
local claiming, state, claimed, fence, msgs, inflight = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
local key, now, ttl = ARGV[1], tonumber(ARGV[2]), tonumber(ARGV[3])

redis.call('LREM', claiming, 1, key)

-- LEFT: oldest first. RPUSH in produce.lua plus a LEFT pop here is FIFO; popping RIGHT would make
-- it a stack and silently reverse the per-key ordering this design exists to guarantee.
local message = redis.call('LMOVE', msgs, inflight, 'LEFT', 'RIGHT')

if not message then
  redis.call('HDEL', state, key)   -- nothing to do after all; back to idle rather than held
  return nil
end

redis.call('HSET', state, key, 'processing')
redis.call('ZADD', claimed, now + ttl, key)

-- The deadline exists from this instant, so a key is never in flight without one. The token is what
-- makes a later reclaim safe: a zombie worker's completion is rejected because its token is stale.
return { message, redis.call('HINCRBY', fence, key, 1) }
```

### `complete` — retire the message, and decide what happens to the key

Three outcomes. If the token is stale the claim was revoked while the worker was busy, so **nothing happens
and the caller is told `0`** — that worker's result no longer counts. Otherwise the in-flight message is
discarded and the lease released, and then the key either goes back on `ready` because more messages
arrived while it was being worked, or becomes idle (its `state` entry deleted).

The token check is what stops a late worker from re-queueing a key the watchdog has already re-queued, which
would put it on `ready` twice and land two workers on one key.

```lua
local state, claimed, fence, msgs, inflight, ready = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6]
local key, token = ARGV[1], tonumber(ARGV[2])

if tonumber(redis.call('HGET', fence, key) or 0) ~= token then
  return 0
end

-- Completing ends the claim, so it must end the token too: a token authorises exactly ONE transition.
-- Without this, a retried `complete` — an at-least-once RPC, a client resending after a timeout — would
-- apply twice and push the key onto `ready` twice, which is two workers on one key.
redis.call('HINCRBY', fence, key, 1)

redis.call('DEL', inflight)
redis.call('ZREM', claimed, key)

if redis.call('LLEN', msgs) > 0 then
  redis.call('HSET', state, key, 'queued')
  redis.call('RPUSH', ready, key)
else
  redis.call('HDEL', state, key)   -- idle is the absence of an entry
end

return 1
```

### `heartbeat` — renew every claim this worker still holds

One call carrying the worker's id and its `(key, token)` pairs, which is why a long-running handler needs no
lease long enough to cover it: the claim is renewed rather than pre-sized. Two guards keep a confused worker
honest — `XX` never resurrects a claim the watchdog has already revoked, and the token check stops a worker
extending a claim that has since been granted to someone else.

It also renews the **worker's own** liveness, and that entry is written *without* `XX`, because this call is
how a worker registers itself. Hence the rule: **heartbeat once before the first `BLMOVE`.** A worker that
claims before it is known leaves a claiming list nothing can ever recover.

Returns how many were renewed. A shortfall against how many were sent names claims this worker has lost,
and it should stop working them.

```lua
local claimed, fence, workers = KEYS[1], KEYS[2], KEYS[3]
local now, ttl, worker = tonumber(ARGV[1]), tonumber(ARGV[2]), ARGV[3]

redis.call('ZADD', workers, now + ttl, worker)

local renewed = 0
for i = 4, #ARGV, 2 do
  local key, token = ARGV[i], tonumber(ARGV[i + 1])
  if tonumber(redis.call('HGET', fence, key) or 0) == token then
    renewed = renewed + redis.call('ZADD', claimed, 'XX', 'CH', now + ttl, key)
  end
end

return renewed
```

### `watchdog` — revoke silent claims and put their keys back

Two sweeps, because there are two ways to lose a key.

**Expired claims.** For everything whose deadline has passed in `claimed`: restore the in-flight message to
the **head** of its FIFO so a retry does not reorder the key, bump the fence so the silent worker's
completion can no longer land, drop the lease, and make the key `queued` and ready again.

**Expired workers.** A worker that died between its `BLMOVE` and `consume.lua` never created a claim, so the
first sweep is blind to it — the key sits in `claiming:<worker>`, still `queued`, with no deadline anywhere.
The second sweep finds workers whose liveness has lapsed and drains their claiming lists back onto `ready`,
tail-to-head so the keys keep their relative order at the front of the queue. Nothing else changes: `state`
is still `queued`, which is exactly what a ready key should be.

It is idempotent, so every pod can run it and no leader election is needed — reclaiming an already reclaimed
key does nothing. It is bounded by `limit` because a script blocks the whole server; whatever is left is
taken by the next sweep, and a sweep that hits its limit should be run again immediately rather than waiting
for the next tick.

What it cannot do is tell "the worker is dead" from "the worker cannot be heard" — see §5.

```lua
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
```

## 3b. Scoping: one namespace per queue

The scripts are namespace-agnostic — they take key names in `KEYS[]`, so the *caller* decides the scope. The
intended one is per queue, everything under a single hash tag:

```
{q:<queue>}:ready            {q:<queue>}:claimed       {q:<queue>}:msgs:<key>
{q:<queue>}:claiming         {q:<queue>}:fence         {q:<queue>}:inflight:<key>
{q:<queue>}:state
```

So the same business key in two queues is two independent keys, with independent ordering and independent
claims. Nothing *enforces* that, though: a caller could pass `{q:a}:ready` with `{q:b}:state` and corrupt
both. The typed facade in §4c is what makes that unrepresentable, which is a decent argument for building it
early rather than "once the scripts settle".

**`watchdog.lua` is the one place the layout is baked in.** It rebuilds `msgs:<key>` and `inflight:<key>`
from an `ARGV` prefix, because which keys have expired is not known until the `ZRANGEBYSCORE` runs. It is
also therefore per queue — N queues means N sweeps, and in cluster mode it cannot be otherwise, since
different queues carry different hash tags and a script may only touch one slot.

**And a worker can block on exactly one queue per connection**, which is the consequence most likely to
shape the API:

| | Multi-queue | Reliable handoff |
|---|---|---|
| `BLMOVE` | no — fixed arity, one source | yes: the key lands in `claiming` atomically |
| `BLMPOP` / `BLPOP` | yes — variadic over keys | no: it pops, with nowhere to put it |

The failure that rules out the second column is not subtle. A worker that pops with `BLMPOP` and dies before
its script runs leaves the key gone from `ready` while `state` still says `queued` — so producers will not
re-push it (that guard is what prevents duplicates) and no `claimed` entry exists for the watchdog to expire.
The key stalls permanently, and detecting it means scanning the `state` hash against list membership.

Three ways out, in the order I would consider them:

1. **One blocking connection per queue per worker.** Keeps `BLMOVE`, costs a connection per queue. Right for
   phase 1.
2. **`BLMPOP` plus a reconciliation sweeper** for `queued`-but-not-in-`ready` keys. Buys multi-queue at the
   price of the stall above and a second watchdog.
3. **One shared `ready` list across queues**, elements encoding `queue:key`. One `BLMOVE` serves everything
   and reliability is intact — at the cost of a single hash tag for the whole system (no sharding across
   queues) and fairness becoming global FIFO rather than per queue.

This belongs in the streaming phase rather than here: a stream invites "subscribe me to these three queues",
and the answer decides whether that is three connections or a shared ready list.

## 4. Why each choice, rather than the obvious one

- **`BLMOVE`, not `BLPOP`.** With `BLPOP` the key exists in no structure between the pop returning and the
  follow-up script running; a worker dying there strands it. `BLMOVE` is blocking *and* an atomic handoff
  into `CLAIMED`, so no such window exists.
- **`LMOVE msgs -> inflight`, not `LPOP`.** The same argument one level down: a popped message is
  unrecoverable if the worker dies, which quietly turns the queue at-most-once.
- **A ZSET of deadlines, not `EXPIRE` per claim.** Redis expiry is invisible — you cannot enumerate what
  vanished without keyspace notifications, which are fire-and-forget, putting us straight back in "was a
  signal missed?" territory. A ZSET makes expiry *queryable*, and re-sweeping is harmless.
- **Per-key deadlines, not a heartbeat inventory to diff.** "Reclaim everything no worker claims to hold"
  requires having heard from *every* live worker first; one late heartbeat and that worker's keys look
  abandoned. Judging each key against its own deadline needs no global knowledge.
- **The heartbeat renews rather than reports.** `ZADD` with many members is one idempotent call, and because
  it is a renewal a long handler simply keeps its key — no lease duration to guess against the longest job,
  which is the usual lease-tuning trap.
- **Lua for every transition.** Otherwise two producers both read `state == idle` and push the key twice — two
  workers on one key. The nastier one: a producer observing `processing` while the worker completes and finds
  nothing waiting, so the key never re-enters `QUEUE` and its messages are stranded. Script atomicity removes
  the class.

## 4b. Scripting: hand-written Lua, behind a typed facade

The scripts above are hand-written rather than generated from Scala. That is a decision, not an omission:

- **No DSL would buy much.** A DSL that records calls can only emit a linear pipeline, and Redis already has
  that without Lua (`MULTI`/`EXEC`). What makes Lua worth using here is branching on values read *inside* the
  script, and expressing that in Scala means a tagless encoding over `LuaExpr[A]` plus a code generator —
  real compiler work, inheriting Lua↔Redis type quirks, and producing scripts that are harder to read in a
  failure than the ones you can paste straight into `valkey-cli`.
- **The typing that pays for itself is at the call boundary**, not in the script body: `zio-redis` gives
  `scriptLoad` / `evalSha` with `Input` and `Output` typeclasses, so a facade like
  `Scripts.claim(queue, key, ttl): IO[E, ClaimResult]` covers the two places errors actually happen —
  parameters and decoding.
- **Prefer Redis 7 Functions (`FUNCTION LOAD` / `FCALL`) to `EVALSHA`** once the instance is ours: the
  library is stored server-side, survives restarts, replicates, and there is no `NOSCRIPT` fallback to get
  wrong. Valkey supports them too.
- **Two constraints while writing them.** A script blocks the single-threaded server, so it must be short and
  bounded — hence `watchdog.lua` taking a `limit` and leaving the rest to the next sweep. And in cluster mode
  every key a script touches must be in one slot, which is what the `{q:<queue>}` hash tag is for; the
  watchdog in particular *builds* key names at runtime, which is only safe because of it.

All five were exercised against the compose instance (Valkey 8.1) end to end — 13 assertions covering FIFO
per key, a key entering `ready` exactly once, a message produced during processing not re-queueing its key,
heartbeat renewing only valid tokens, both watchdog sweeps, and a zombie's completion rejected on its stale
token without duplicating the key in `ready`.

**That run found a design bug worth recording.** The fence was originally bumped when a claim was *granted*
and when one was *revoked*, but not when one was *completed* — so a repeated `complete` with the same token
applied twice and pushed its key onto `ready` twice, which is two workers on one key. An at-least-once RPC
retry or a client resending after a timeout is enough to trigger it. The rule the scripts now follow is
simply stated: **a token authorises exactly one transition.** Granting, revoking and completing all advance
the generation.

## 4c. Calling them from Scala

Two layers. The namespace makes mismatched keys unrepresentable; the facade turns each script into one method
whose result the caller is forced to handle.

```scala
/** Every key one queue owns. The single hash tag is what lets a script touch them all in cluster mode. */
final case class Namespace(queue: String):
  private val tag = s"{q:$queue}"
  val ready       = s"$tag:ready"
  val claimed     = s"$tag:claimed"
  val state       = s"$tag:state"
  val fence       = s"$tag:fence"
  val workers     = s"$tag:workers"
  val prefix      = tag                             // watchdog.lua rebuilds the rest from this
  def msgs(key: String)         = s"$tag:msgs:$key"
  def inflight(key: String)     = s"$tag:inflight:$key"
  def claiming(worker: String)  = s"$tag:claiming:$worker"

/** One claim: the message to run, and the token that proves the claim is still ours. */
final case class Claim(key: String, message: Chunk[Byte], token: Long)

trait Scripts:
  def produce(ns: Namespace, key: String, payload: Chunk[Byte]): IO[RedisError, Long]
  def claim(ns: Namespace, worker: WorkerId, timeout: Duration): IO[RedisError, Option[Claim]]
  def complete(ns: Namespace, claim: Claim): IO[RedisError, Boolean]
  def heartbeat(ns: Namespace, worker: WorkerId, claims: Chunk[Claim]): IO[RedisError, Long]
  def reclaim(ns: Namespace, limit: Int): IO[RedisError, Reclaimed]
```

The namespace is an argument rather than something the instance closes over, so **one `Scripts` serves every
queue**: no per-queue wiring, no layer per queue, and a worker that spans queues (§3b) needs nothing new. The
cost is that `complete` and `heartbeat` can now be handed a namespace that does not match the claim they were
given — the very mismatch the type was introduced to prevent. If that turns out to matter, the fix is to put
the namespace inside `Claim` and drop the parameter from those two, which makes the pairing unforgeable
again.

A representative implementation — the shape matters, the exact `zio-redis` method names and `Output`
builders should be checked against the pinned version:

```scala
def produce(ns: Namespace, key: String, payload: Chunk[Byte]): IO[RedisError, Long] =
  redis
    .evalSha(sha.produce, keys = Chunk(ns.ready, ns.state, ns.msgs(key)), args = Chunk(key, payload))
    .returning[Long]

def claim(ns: Namespace, worker: WorkerId, timeout: Duration): IO[RedisError, Option[Claim]] =
  // the block cannot live in a script; BLMOVE is what makes the handoff atomic, and the per-worker
  // destination is what makes a death in the seam recoverable
  redis.blMove(ns.ready, ns.claiming(worker), Side.Left, Side.Right, timeout).flatMap:
    case None      => ZIO.none                       // nothing became ready before the deadline
    case Some(key) =>
      redis
        .evalSha(
          sha.consume,
          keys = Chunk(ns.claiming, ns.state, ns.claimed, ns.fence, ns.msgs(key), ns.inflight(key)),
          args = Chunk(key, now.toMillis.toString, ttl.toMillis.toString),
        )
        .returning[Option[(Chunk[Byte], Long)]]
        .map(_.map((message, token) => Claim(key, message, token)))
```

**Key order is part of the contract** — the scripts read `KEYS[1]`, `KEYS[2]` positionally, so a swapped pair
is a silent corruption rather than an error. That is the single best reason to have the facade be the only
caller. Keep `args` homogeneous too (encode everything to bytes or everything to strings): the client's
`Input` typeclass applies one type to the whole `Chunk`.

### Holding the blocking connection

`BLMOVE` occupies its connection for as long as it waits, so it **cannot share a pooled connection** with
anything else — the pool would starve behind a parked command. One dedicated connection per claimer, and the
worker id is that connection's identity.

```scala
/**
 * A registered idle connection, bound to one queue. Stateful and single-apply-at-a-time — it owns the
 * connection `BLMOVE` parks on — which is why it is a handle rather than a set of functions taking a
 * worker id: a `Claimer` that exists is a worker already registered in `workers`.
 */
trait Claimer:
  /** One claim attempt, bounded by the caller's deadline. At most one runs on this connection at a time. */
  def dequeue(timeout: Duration): IO[RedisError, Option[Claim]]

object Claimer:
  /** Registers the worker, starts renewing it, and gives back the handle for the life of the scope. */
  def make(ns: Namespace, worker: WorkerId): ZIO[Scope, RedisError, Claimer] =
    for
      redis <- blockingConnection                             // its own, never a pooled one
      _     <- scripts.heartbeat(ns, worker, Chunk.empty)     // registration: awaited, not forked
      _     <- scripts.heartbeat(ns, worker, held).repeat(tick).forkScoped
      _     <- ZIO.addFinalizer(release(redis, ns, worker) *> deregister(ns, worker))
    yield new Claimer:

      def dequeue(timeout: Duration): IO[RedisError, Option[Claim]] =
        ZIO
          .uninterruptibleMask: restore =>
            restore(redis.blMove(ns.ready, ns.claiming(worker), Side.Left, Side.Right, timeout)).flatMap:
              case None      => ZIO.none
              case Some(key) => scripts.consume(ns, worker, key)  // uninterruptible: we hold the key
          .onInterrupt(release(redis, ns, worker))

  /** Give back whatever this connection was holding — at most one key, since it makes one BLMOVE at a time. */
  private def release(redis: Redis, ns: Namespace, worker: WorkerId): UIO[Unit] =
    redis.lMove(ns.claiming(worker), ns.ready, Side.Right, Side.Left).ignore.unit
```

Four things that sketch is trying to say.

**The claim happens in the fiber that will use it.** The RPC handler does its own `BLMOVE` rather than a
background loop claiming into a buffer that handlers draw from. That is the whole lesson of
[the toolkit's `PollConsumer`](../../../homelab-toolkit-zio/docs/sessions/2026-08-22-pollconsumer-orphans.md):
separating the claim from the claimant is what creates work claimed for a caller that no longer exists, and
no amount of bookkeeping fully repairs it. Here the claimant *is* the worker, so the problem does not arise.

**A cancelled request cannot un-send a network call.** If the client's deadline expires while parked in
`BLMOVE`, the command may already have moved a key server-side — interrupting the fiber does not undo that.
So the key can be sitting in `claiming:<worker>` with the handler gone. That is what `onInterrupt(release)`
is for, and it is the fast path.

**Worker liveness is the backstop, not the mechanism.** The `workers` sweep only fires when dkq itself dies —
a live pod keeps renewing, so a key stranded by a cancelled request would sit there until the pod restarted
if the handler did not clean up after itself. Fast path releases; slow path catches what a crash prevented.

**The mask has no gap.** `restore` covers only the wait; once `blMove` returns a key, `consume` runs
uninterruptibly, so there is no window where a key is held with no claim recorded — the same discipline, and
for the same reason, as `PollConsumer.consume`.

**Why `Claimer` is a handle and `Scripts` is not.** `Scripts` is stateless and shared, so it takes a
`Namespace` per call. A claimer owns something — a connection, an identity, a registration — and those three
must agree, so they are bundled: **a `Claimer` that exists is a worker already registered in `workers`**, and
its `dequeue` cannot be handed a worker id that never announced itself. Same argument as `Namespace`, applied
to the thing that has a lifecycle.

On a graceful stop the finalizer does one more thing worth doing: `ZREM` the worker from `workers` after
draining, so nothing is left for a sweep to find and a rolling restart does not park that connection's key
for a full TTL.

What each call means, and what the caller is obliged to do with the answer:

| Script | `KEYS`, in order | `ARGV` | Returns | The caller must |
|---|---|---|---|---|
| `produce` | ready, state, msgs(key) | key, payload | the key's queue depth | nothing — the depth is a metric, not a decision |
| `consume` | claiming, state, claimed, fence, msgs(key), inflight(key) | key, now, ttl | `{message, token}` or nil | nil means that key had nothing to work and was released: **claim again within the caller's remaining deadline** — it is neither an error nor "the queue is empty", since other keys may have work. Otherwise **keep the token**: every later call needs it |
| `complete` | state, claimed, fence, msgs(key), inflight(key), ready | key, token | 1 applied, 0 stale | **0 means the claim was revoked while you worked.** Discard the result, do not retry, do not touch that key — somebody else may own it now |
| `heartbeat` | claimed, fence, workers | now, ttl, worker, then (key, token)… | how many claims were renewed | **call it once before the first claim** — this is also registration. Compare the count with how many were sent: a shortfall names claims you have lost, and you should stop working them |
| `watchdog` | claimed, state, ready, fence, workers | now, limit, prefix | `{keys reclaimed, workers recovered}` | log and count both — reclaim rate is the health metric here, and a non-empty worker list means something died mid-claim. If either count equals `limit`, sweep again immediately rather than waiting for the next tick |

Retrying on nil is safe rather than a spin: `BLMOVE` has already removed the key from `ready` and the script
does `HDEL state` before returning, so each stale entry is consumed exactly once and the queue drains its own
debris — and every iteration blocks, so an idle loop costs a parked connection, not CPU. The branch is mostly
defensive: a key only enters `ready` with a message, and both `complete` and `watchdog` check before
re-queueing, so the realistic paths are out-of-band — a dead-letter policy dropping a poison message while its
key sits queued, an administrative purge, or a bug. Which is the argument for a quiet retry: it stops a wrong
assumption elsewhere from becoming an outage here.

The `complete` row is the one worth reading twice: it is the only place the design tells a worker *"your work
no longer counts"*, and a caller that ignores the boolean re-introduces exactly the double-processing the
fencing token exists to prevent.

## 5. Hazards that survive the design

1. **A missed heartbeat means "I cannot hear you", not "you are dead".** A GC pause or a partition lets the
   watchdog reclaim a key whose handler is still running, and then two workers *are* running it. No timeout
   scheme avoids this — leases are not locks. `fence:<key>` is what keeps it survivable: the zombie's
   completion, and ideally its side effect, is rejected as stale. The honest guarantee is **at most one
   *effective* worker per key**, not at most one running — hence handlers stay idempotent.
2. **A poison message blocks its key forever.** Worse here than in an unordered queue: the failing message
   sits at the head of `msgs:<key>`, so every later message for that key waits behind it through every retry.
   Needs an attempt counter and a policy — dead-letter the message and let the key proceed, or park the key
   and alarm. Decide before the first deployment, not after it.
3. **Cluster mode needs hash tags.** A Lua script may only touch keys in one slot, so names have to be written
   `{<queue>}:msgs:<key>` to co-locate them. Cheap now, invasive later.
4. **A cancelled `Dequeue` is a cancelled claim.** A client deadline or disconnect interrupts the server fiber
   mid-claim — see
   [`homelab-toolkit-zio/docs/sessions/2026-08-22-pollconsumer-orphans.md`](../../../homelab-toolkit-zio/docs/sessions/2026-08-22-pollconsumer-orphans.md)
   for how expensive that is when the claim does not happen in the fiber that will do the work. Here it is
   cheap for the same reason it is cheap in a single-element design: `BLMOVE` returns to the fiber that will
   run the work, and if that fiber dies the deadline reclaims the key.

## 6. What Redis costs, against Postgres

| | Redis | Postgres |
|---|---|---|
| blocking dequeue | native (`BLMOVE`) | `LISTEN`/`NOTIFY` hint + poll floor |
| per-key exclusivity | structural — a key is in `QUEUE` at most once | a `keys` table claimed with `SKIP LOCKED` |
| lease / recovery | hand-rolled ZSET + watchdog | `claimed_until` column + reaper |
| multi-structure atomicity | Lua scripts | one transaction |
| durability | async replication, AOF `everysec` — a failover can lose ~1s of acknowledged writes | synchronous replicas + PITR, already running under CNPG |
| operational cost | **a new stateful service** — not in the cluster today | already deployed |

The durability row is the one that decides it. The rest is taste and line count; "we acknowledged your job
and then lost it" is a different kind of statement to make about a job queue.

## 7. What the POC should settle

1. Does the keyed shape hold up end to end — enqueue, blocking dequeue, per-key serial, concurrency across
   keys — over gRPC and across pods?
2. What does reclaim cost in practice: how often does the watchdog fire under normal churn, and does the
   fencing token ever reject anything?
3. Is head-of-line blocking per key acceptable for the workloads in mind, or does a key need a
   parallelism > 1 mode?
4. Only then: is the durability trade acceptable, or does this shape want re-implementing on Postgres with
   the same state machine and `SKIP LOCKED` doing what `BLMOVE` did?

Two smaller ones the scripts leave open. **`fence` grows one entry per key, forever** — deleting a key's
entry when it goes idle would bound it, at the cost of a token value being reusable after a full idle
period, which a long-paused zombie could in principle collide with. And **`inflight` may not need to exist**:
`consume` could peek the head of `msgs` and let `complete` pop it, which would delete a structure and remove
the watchdog's restore-to-head step — the reordering hazard would stop existing rather than being handled.
Both are cheap to change now and awkward later.

A port boundary is what keeps (4) open: the state machine above is substrate-agnostic, and the only thing
Redis really contributes is that the waiting is free.
