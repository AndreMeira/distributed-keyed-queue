---
title: "What dkq keeps in Redis, and what each structure is for"
type: architecture
status: current
updated: 2026-09-05
tags: [redis, keys, data-structures, lua, claims, ordering, streams]
---

# What dkq keeps in Redis

Every piece of state lives in Redis; dkq pods hold nothing but connections. This is the whole layout, why
each structure has the type it has, and which script touches it.

`Namespace` is the single place these names are built. Nothing else in the codebase constructs a key name,
apart from `consume.lua` and `watchdog.lua`, which rebuild the per-key ones at runtime — see
[Cluster](#one-bucket-one-slot).

## The layout

Everything a queue owns is prefixed `{w:<bucket>}:q:<queue>`, where the bucket is
`hash(queue) % DKQ_WAKE_BUCKETS` and decides which cluster slot the queue lives in. Six structures belong to
the queue and three to a key inside it; the seventh, `wake`, belongs to the bucket and is shared by every
queue in it. `{Q}` below is one queue's prefix, `{W}` its bucket's:

| key | type | maps | written by |
|---|---|---|---|
| `{Q}:ready` | zset | key → its place in line; keys with work and nobody working them | produce, consume, complete, watchdog |
| `{Q}:seq` | string | the counter that scores `ready` | produce, complete, watchdog |
| `{Q}:claimed` | zset | key → lease deadline, unix millis | consume, complete, watchdog |
| `{Q}:fence` | hash | key → claim counter | consume, complete, watchdog |
| `{Q}:attempts` | hash | **message id** → delivery count | consume, complete |
| `{Q}:delayed` | zset | key → when it may be worked again | complete, watchdog |
| `{W}:wake` | **stream** | one entry per key made claimable, naming its queue | produce, complete, watchdog |
| `{Q}:msgs:<key>` | list | that key's message ids, producer order | produce, complete |
| `{Q}:payloads:<key>` | hash | message id → the message | produce, consume, complete |
| `{Q}:owned:<key>` | set | ids the live claim holds and has not settled | consume, complete, watchdog |

## Why the types are what they are

**`ready` is a sorted set, scored by arrival.** It is a FIFO across keys — `ZPOPMIN` in `consume.lua`
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

**`wake` is a stream, and there is one per bucket.** A stream rather than pub/sub because a reader that
reconnects resumes from the id it holds, where a subscriber would simply have missed whatever arrived while
it was away — and a missed wake is a consumer asleep beside claimable work. Tagged by bucket rather than by
queue because a script may not touch two cluster slots: sharing the *bucket's* hash tag with the keys it
announces is what lets the entry be appended *in the same call* that made the key claimable, so no crash can
land between the two. Shared by every queue in the bucket because a listener's `XREAD` names the streams it
was issued with — a stream per queue means a set that grows as queues are served, and a queue asked for
while a read is in flight goes unheard until that read returns.

Entries are trimmed with `MAXLEN ~ 1000` on every append and carry two fields: `queue`, which is what routes
the entry to the right waiting consumers now that the stream name no longer says, and `key`, which is for a
human reading `XRANGE` — a consumer claims whatever is at the head rather than the key it was told about.

The trim is a budget shared by the bucket, and it is denominated in entries rather than time: at a few
thousand appends a second, a thousand entries is a fraction of a second of history. That only matters to a
listener that is *away* — one reading continuously is a handful of entries behind — and a listener that
reconnects raises every local signal before it resumes, precisely because `XREAD` cannot report having been
trimmed past.

## What a message's life touches

**Enqueue** — `HSETNX payloads id`, and if that is new, `RPUSH msgs id` and, when the key is in none of
`ready`, `claimed` or `delayed`, `ZADD ready <INCR seq> key`. The `HSETNX` is what makes a repeated enqueue
idempotent: the same id twice for one key is one message, for as long as it is queued. The three membership
checks are what stop a key being queued while it is being worked — a key in `ready` twice is two consumers
on one key, which is the one thing this design forbids.

**Claim** — one call. `consume.lua` does `ZPOPMIN ready` itself, then takes the first N ids with
`LRANGE msgs 0 n-1`, `SADD`s them to `owned`, writes the lease into `claimed`,
advances `fence`, counts an attempt each, and reads the payloads with `HMGET`. Because the pop and the grant
are one script, **a key is either in `ready` or claimed** — there is no in-between state for anything to
recover, which is why there is no per-connection holding list here any more.

**Claimed messages do not move.** They stay in `msgs`, in producer order, with `owned` recording which of
them the claim holds. That is the load-bearing decision here, and three things fall out of it: a nack has
nothing to put back, a crash has nothing to repair, and producer order cannot be disturbed by the order in
which a consumer settles.

**Settle** — `complete.lua` checks the token against `fence`, then per named id: `SREM owned` and, if
acknowledged, `LREM msgs` + `HDEL payloads` + `HDEL attempts`. A nack removes only the ownership, and may
`ZADD delayed GT` to ask the key to wait. When `owned` is empty the claim is over: `fence` advances,
`claimed` is cleared, and the key goes back to `ready` with a *fresh* score — it has been served, so it
queues behind everything still waiting — or stays out of it, held by `delayed` or by having nothing left.

**Every addition to `ready` appends a wake.** `produce.lua`, `complete.lua` and both watchdog sweeps append
to `wake` in the same script, and only where the key actually became claimable — a nacked key parked in
`delayed` announces nothing, because its wake comes later, from the sweep that releases it.

## Two rules that are easy to break

**The fence is checked on every settle and advanced only when the claim ends.** A claim settled piece by
piece needs its token to stay valid across several calls, so the counter cannot move mid-claim. What stops
a settle applying twice is `SREM` finding nothing the second time; what stops a zombie settling is the
counter moving when the claim ends — including when the watchdog ends it.

**Whether a key is claimable is two questions, not one.** A partial nack can set a backoff in `delayed`
while the claim is still alive, so both `complete.lua`'s claim-end and the watchdog's reclaim must check
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

## One bucket, one slot

Every name above carries its bucket's `{w:<bucket>}` hash tag, so a queue's keys — and the wake stream that
announces them — hash to one cluster slot and a script may touch them all. `consume.lua` and `watchdog.lua`
build `msgs:<key>`, `payloads:<key>` and `owned:<key>` at runtime from `prefix` rather than receiving them
in `KEYS` — legal only because the tag guarantees the same slot, and unavoidable for `consume.lua`, which
does not know which key it has until it pops one. That is why both take `prefix` as an argument.

The same rule is what decides where `wake` lives: a stream tagged differently from the keys it announces
would be a different slot, so the append could not share a script with the push that made the key claimable.
Tagging both by bucket is what keeps them together.

The consequence is that **a bucket lives on one node**: sharding spreads buckets, never one queue and never
one bucket. At the default of one bucket that is the whole service, which is right for a single node and no
use in a cluster. See [`redis-cluster.md`](redis-cluster.md).
