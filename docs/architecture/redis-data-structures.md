---
title: "What dkq keeps in Redis, and what each structure is for"
type: architecture
status: current
updated: 2026-08-30
tags: [redis, keys, data-structures, lua, claims, ordering]
---

# What dkq keeps in Redis

Every piece of state lives in Redis; dkq pods hold nothing but connections. This is the whole layout, why
each structure has the type it has, and which script touches it.

`Namespace` is the single place these names are built. Nothing else in the codebase constructs a key name,
apart from `watchdog.lua`, which rebuilds three of them at runtime — see [Cluster](#one-queue-one-slot).

## The layout

Everything a queue owns is prefixed `{q:<queue>}`. Seven structures belong to the queue, four to something
inside it:

| key | type | maps | written by |
|---|---|---|---|
| `{q:Q}:ready` | list | keys with work and nobody working them | produce, complete, watchdog |
| `{q:Q}:state` | hash | key → `queued` \| `processing` (absent = idle) | produce, consume, complete, watchdog |
| `{q:Q}:claimed` | zset | key → lease deadline, unix millis | consume, complete, watchdog |
| `{q:Q}:fence` | hash | key → claim counter | consume, complete, watchdog |
| `{q:Q}:attempts` | hash | **message id** → delivery count | consume, complete |
| `{q:Q}:workers` | zset | worker → liveness deadline | heartbeat, watchdog |
| `{q:Q}:delayed` | zset | key → when it may be worked again | complete, watchdog |
| `{q:Q}:msgs:<key>` | list | that key's message ids, producer order | produce, complete |
| `{q:Q}:payloads:<key>` | hash | message id → the message | produce, consume, complete |
| `{q:Q}:owned:<key>` | set | ids the live claim holds and has not settled | consume, complete, watchdog |
| `{q:Q}:claiming:<worker>` | list | keys in transition between `BLMOVE` and `consume.lua` | consume, watchdog |

## Why the types are what they are

**`ready` is a list, not a set** — because `BLMOVE` is what makes a dequeue block without polling, and it
only works on lists. That single command is the reason the whole design is built around lists at all.

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

**`claiming` is per worker** because it is the only record that a key was taken from `ready` but not yet
claimed. If the worker dies in that instant, nothing else knows the key exists; the watchdog finds it by
draining the list of a worker whose liveness has lapsed.

## What a message's life touches

**Enqueue** — `HSETNX payloads id`, and if that is new, `RPUSH msgs id` and, when the key is idle,
`HSET state queued` + `RPUSH ready key`. The `HSETNX` is what makes a repeated enqueue idempotent: the same
id twice for one key is one message, for as long as it is queued.

**Claim** — `BLMOVE ready → claiming:<worker>` blocks; then `consume.lua` takes the first N ids with
`LRANGE msgs 0 n-1`, `SADD`s them to `owned`, sets `state processing`, writes the lease into `claimed`,
advances `fence`, counts an attempt each, and reads the payloads with `HMGET`.

**Claimed messages do not move.** They stay in `msgs`, in producer order, with `owned` recording which of
them the claim holds. That is the load-bearing decision here, and three things fall out of it: a nack has
nothing to put back, a crash has nothing to repair, and producer order cannot be disturbed by the order in
which a consumer settles.

**Settle** — `complete.lua` checks the token against `fence`, then per named id: `SREM owned` and, if
acknowledged, `LREM msgs` + `HDEL payloads` + `HDEL attempts`. A nack removes only the ownership, and may
`ZADD delayed GT` to ask the key to wait. When `owned` is empty the claim is over: `fence` advances,
`claimed` is cleared, and the key goes back to `ready`, into `delayed`, or idle.

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

## The watchdog's three sweeps

1. **Lapsed claims** — `ZRANGEBYSCORE claimed -inf now`: `DEL owned:<key>`, advance `fence`, clear the
   lease, and re-ready the key unless `delayed` says otherwise. Nothing moves, because nothing had moved.
2. **Dead workers** — `ZRANGEBYSCORE workers -inf now`: drain that worker's `claiming` list back to `ready`
   and forget the worker. This is the only thing that can recover a key lost between `BLMOVE` and
   `consume.lua`.
3. **Elapsed backoffs** — `ZRANGEBYSCORE delayed -inf now`: back onto `ready`.

## One queue, one slot

Every name above carries the `{q:<queue>}` hash tag, so a queue's keys hash to one cluster slot and a script
may touch them all. `watchdog.lua` builds `owned:<key>` and `claiming:<worker>` at runtime from `prefix`
rather than receiving them in `KEYS` — legal only because the tag guarantees the same slot, which is why the
sweep takes `prefix` as an argument at all.

The consequence is that **a queue lives on one node**: sharding spreads queues, never one queue. See
[`redis-cluster.md`](redis-cluster.md).
