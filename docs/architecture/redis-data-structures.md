---
title: "What dkq keeps in Redis, and what each structure is for"
type: architecture
status: current
updated: 2026-09-26
tags: [redis, keys, data-structures, lua, claims, ordering, streams]
---

# What dkq keeps in Redis

Every piece of state lives in Redis; dkq pods hold nothing but connections. This is the whole layout, why
each structure has the type it has, and which script touches it.

`QueueKeys` builds the queue's names and `LockKeys` the lock's. Nothing else in the codebase constructs a
key name, apart from `claim.lua`, `sweep.lua` and the lock's `trim.lua`, which rebuild per-key ones at
runtime — see [Cluster](#one-partition-one-slot).

## The layout

Everything a queue owns is prefixed `{p:<partition>}:v3:q:<queue>`, where the partition is
`hash(queue) % 16` (the partition count is a constant of the code) and decides which cluster slot the queue
lives in, and `v3` is the schema version every key carries
([`../research/schema-versioned-keys.md`](../research/schema-versioned-keys.md)). Six structures belong to
the queue and three to a key inside it; the seventh, `wake`, belongs to the partition and is shared by every
queue in it. `{Q}` below is one queue's prefix, `{W}` its partition's (version included in both):

| key | type | maps | written by |
|---|---|---|---|
| `{Q}:ready` | zset | key → its place in line; keys with work and nobody working them | produce, consume, complete, watchdog |
| `{Q}:seq` | string | the counter that scores `ready` | produce, complete, watchdog |
| `{Q}:claimed` | zset | key → lease deadline, unix millis | consume, complete, watchdog |
| `{Q}:fence` | hash | key → claim counter | consume, complete, watchdog |
| `{Q}:attempts` | hash | **message id** → delivery count | consume, complete |
| `{Q}:delayed` | zset | key → when it may be worked again | complete, watchdog |
| `{W}:wake` | **stream** | one entry per key made claimable: `kind` `q`, the queue's name, the key | produce, complete, watchdog |
| `{Q}:msgs:<key>` | list | that key's message ids, producer order | produce, complete |
| `{Q}:payloads:<key>` | hash | message id → the message | produce, consume, complete |
| `{Q}:owned:<key>` | set | ids the live claim holds and has not settled | consume, complete, watchdog |

## Why the types are what they are

**`ready` is a sorted set, scored by arrival.** It is a FIFO across keys — `ZPOPMIN` in `claim.lua`
serves whichever key has waited longest — and being a set is what makes "is this key already queued" the
structure's own property rather than a separate hash to keep in step. It was a list while `BLMOVE` needed
one, and briefly afterwards; a set also makes claiming a *named* key O(log N), which is what any affinity
scheme would need.

**The score is a counter, not a clock.** `{Q}:seq` is `INCR`ed once per key that becomes claimable.
A timestamp is the obvious score and is wrong: at even moderate rates several keys become claimable within
the same millisecond, and `ZPOPMIN` breaks a tie by *member name* — so cross-key ordering would quietly
become alphabetical. Measured here, 200 keys enqueued back to back produced 133 distinct millisecond
scores. A counter cannot tie, and it makes every writer agree on what "older" means without agreeing on a
clock.

**Idle is the absence of the key**, everywhere. There is no structure that says what a key is doing: it is
claimable if it is in `ready`, held if it is in `claimed`, waiting if it is in `delayed`, and idle if it is
in none of them. A `state` hash used to say so explicitly, but nothing ever branched on its value — it was
read in one place, for existence only, and three membership checks answer that question from structures
that have to be right anyway.

**`msgs` holds ids, `payloads` holds messages.** The split is what lets a script address a message by name:
Redis cannot read inside a serialised protobuf, so if the list held payloads then "drop this message" could
only ever mean "drop the first N". With ids in the list, it is one `LREM`. It also makes a look at the
backlog cheap — `LRANGE msgs 0 n` is ids only, and payloads are fetched only if wanted.

**`owned` is a set, not a list or a counter.** A claim is over when it is empty, and a settle may arrive
twice; `SREM` answers "did this claim own it, and had it already been settled?" in one operation, which is
what makes a retried settle harmless.

**`attempts` is keyed by message id, not by key.** A claim may own several messages at once, so "how many
times has this been delivered" stops being a question about the key. A nacked message keeps its count and
climbs on redelivery, which is what makes a poison message visible.

**`claimed` and `delayed` are sorted sets** because both are swept by "everything due before now", which is
`ZRANGEBYSCORE` — the operation they exist to serve.

**`wake` is a stream, there is one per partition, and both APIs announce on it.** Every entry carries a
`kind` field — `q` for a queue with work, `l` for a lock that came free — and the listener routes on that
rather than on which stream it arrived from, so the two sinks stay separate while the partition holds one
stream, one slot and one blocking connection instead of two. A stream rather than pub/sub because a reader that
reconnects resumes from the id it holds, where a subscriber would simply have missed whatever arrived while
it was away — and a missed wake is a consumer asleep beside claimable work. Tagged by partition rather than by
queue because a script may not touch two cluster slots: sharing the *partition's* hash tag with the keys it
announces is what lets the entry be appended *in the same call* that made the key claimable, so no crash can
land between the two. Shared by every queue in the partition because a listener's `XREAD` names the streams it
was issued with — a stream per queue means a set that grows as queues are served, and a queue asked for
while a read is in flight goes unheard until that read returns.

Entries are trimmed with `MAXLEN ~ 1000` on every append and carry two fields: `queue`, which is what routes
the entry to the right waiting consumers now that the stream name no longer says, and `key`, which is for a
human reading `XRANGE` — a consumer claims whatever is at the head rather than the key it was told about.

The trim is a budget shared by the partition, and it is denominated in entries rather than time: at a few
thousand appends a second, a thousand entries is a fraction of a second of history. That only matters to a
listener that is *away* — one reading continuously is a handful of entries behind — and a listener that
reconnects announces every local queue before it resumes, precisely because `XREAD` cannot report having been
trimmed past.

## What a message's life touches

**Enqueue** — `HSETNX payloads id`, and if that is new, `RPUSH msgs id` and, when the key is in none of
`ready`, `claimed` or `delayed`, `ZADD ready <INCR seq> key`. The `HSETNX` is what makes a repeated enqueue
idempotent: the same id twice for one key is one message, for as long as it is queued. The three membership
checks are what stop a key being queued while it is being worked — a key in `ready` twice is two consumers
on one key, which is the one thing this design forbids.

**Claim** — one call. `claim.lua` does `ZPOPMIN ready` itself, then takes the first N ids with
`LRANGE msgs 0 n-1`, `SADD`s them to `owned`, writes the lease into `claimed`,
advances `fence`, counts an attempt each, and reads the payloads with `HMGET`. Because the pop and the grant
are one script, **a key is either in `ready` or claimed** — there is no in-between state for anything to
recover, which is why there is no per-connection holding list here any more.

**Claimed messages do not move.** They stay in `msgs`, in producer order, with `owned` recording which of
them the claim holds. That is the load-bearing decision here, and three things fall out of it: a nack has
nothing to put back, a crash has nothing to repair, and producer order cannot be disturbed by the order in
which a consumer settles.

**Settle** — `settle.lua` checks the token against `fence`, then per named id: `SREM owned` and, if
acknowledged, `LREM msgs` + `HDEL payloads` + `HDEL attempts`. A nack removes only the ownership, and may
`ZADD delayed GT` to ask the key to wait. When `owned` is empty the claim is over: `fence` advances,
`claimed` is cleared, and the key goes back to `ready` with a *fresh* score — it has been served, so it
queues behind everything still waiting — or stays out of it, held by `delayed` or by having nothing left.

**Renew** — `renew.lua` asks two questions per named key and writes only if both hold: the token against
`fence`, and `ZSCORE claimed` for whether the claim is still there at all. It then `ZADD claimed XX` with
the new deadline. A key failing either is returned as stale rather than renewed, which the consumer has to
read as "stop working this". Nothing moves: a heartbeat touches the deadline and nothing else.

The two questions are different — an owner that has been superseded versus a claim the watchdog has already
taken away — and both have to be asked, because the deadline itself says nothing. A renewal arriving inside
the same millisecond as the one before it writes the deadline the claim already had, and is a renewal like
any other.

**Every addition to `ready` appends a wake.** `enqueue.lua`, `settle.lua` and both watchdog sweeps append
to `wake` in the same script, and only where the key actually became claimable — a nacked key parked in
`delayed` announces nothing, because its wake comes later, from the sweep that releases it.

## Two rules that are easy to break

**The fence is checked on every settle and advanced only when the claim ends.** A claim settled piece by
piece needs its token to stay valid across several calls, so the counter cannot move mid-claim. What stops
a settle applying twice is `SREM` finding nothing the second time; what stops a zombie settling is the
counter moving when the claim ends — including when the watchdog ends it.

**Whether a key is claimable is two questions, not one.** A partial nack can set a backoff in `delayed`
while the claim is still alive, so both `settle.lua`'s claim-end and the watchdog's reclaim must check
`ZSCORE delayed` before pushing to `ready`. Push in both places and the key lands on `ready` twice, and two
consumers claim it — the fence stops the loser corrupting anything, but it works for nothing.
`QueueStoreSpec` has a regression test for exactly this.

## The watchdog's two sweeps

1. **Lapsed claims** — `ZRANGEBYSCORE claimed -inf now`: `DEL owned:<key>`, advance `fence`, clear the
   lease, and score the key back into `ready` unless `delayed` says otherwise. Nothing moves, because
   nothing had moved.
2. **Elapsed backoffs** — `ZRANGEBYSCORE delayed -inf now`: back into `ready`, with a fresh score.

There used to be a third, draining the holding list of a connection that died mid-claim. With the claim in
one script there is no such list and no such moment: **the lease is the only thing that expires.**

## One partition, one slot

Every name above carries its partition's `{p:<partition>}` hash tag, so a queue's keys — and the wake stream that
announces them — hash to one cluster slot and a script may touch them all. Only the braces are hashed, so
the `v3` that follows the tag names the schema without moving anything between slots. `claim.lua` and `sweep.lua`
build `msgs:<key>`, `payloads:<key>` and `owned:<key>` at runtime from `prefix` rather than receiving them
in `KEYS` — legal only because the tag guarantees the same slot, and unavoidable for `claim.lua`, which
does not know which key it has until it pops one. That is why both take `prefix` as an argument.

The same rule is what decides where `wake` lives: a stream tagged differently from the keys it announces
would be a different slot, so the append could not share a script with the push that made the key claimable.
Tagging both by partition is what keeps them together.

The consequence is that **a partition lives on one node**: sharding spreads partitions, never one queue and never
one partition. A single server uses one partition and holds everything; a cluster uses sixteen and spreads
them. See [`redis-cluster.md`](redis-cluster.md).

## The lock's structures

What moves between them is [`lock-mechanics.md`](lock-mechanics.md).

The lock API shares the store but none of the queue's structures. Everything it owns is prefixed
`{p:<partition>}:v3:l`, where the partition is `hash(lock) % 16` — the queue's tag space, so a lock's keys
and the stream announcing it hash to one slot and a script may touch them together
([`redis-cluster.md`](redis-cluster.md)). `{L}` below is one partition's prefix, and the structures it names
are shared by every lock in that partition, with the lock's name as a member or field:

| key | type | maps | written by |
|---|---|---|---|
| `{L}:held` | zset | lock name → lease deadline, unix millis | acquire, grant, try, refresh, release, trim |
| `{L}:tokens` | hash | lock name → the live holder's fence token | acquire, grant, try, release, trim |
| `{L}:fence` | string | one counter for the partition | acquire, grant, try |
| `{L}:waiting` | zset | lock name → the latest ticket deadline in its waiters list | acquire, grant, abandon, try, trim |
| `{L}:waiters:<name>` | list | tickets `id:deadline`, arrival order; exists only while someone queues | acquire, grant, abandon, try |
| `{p:<partition>}:v3:wake` | stream | one entry per lock freed, `kind` `l` and the name | release, trim |

**The fence is one counter per partition, and `tokens` is why it can be.** Fences need only increase per
lock, which a globally increasing number satisfies a fortiori — while a per-lock counter could never be
deleted, since the next grant must exceed every fence ever issued for the name. So the counter is the one
key that outlives holds; `tokens` names the live holder and dies with its hold. The counter also mints
ticket ids — identity only, never a fence, which is always `INCR`ed at grant.

**The waiters list is the fairness.** Grants follow ticket order among tickets still within their
patience; a waiter's patience is its ticket's deadline, so dead waiters are pruned from the head inline —
no heartbeats, no registry. `waiting` is the index the trim uses to find lists whose every ticket expired
unseen.

**Nothing sweeps the lock.** Acquire is named, so an expired lease is reclaimed inline by the next grant;
the trim that runs on a timer is hygiene for holds and waiter lists nobody will ask for again, not
liveness. The full contract is [`lock-guarantees.md`](lock-guarantees.md).
