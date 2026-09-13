---
title: "Redis Cluster — the layout, the per-slot listener, and the fixture that proves them"
type: architecture
status: current
updated: 2026-09-11
tags: [redis, cluster, sharding, hash-tag, lettuce, lua]
---

# Redis Cluster

Set `cluster = true` (or `DKQ_CLUSTER=true`) and `redis-url` is read as a seed node instead of a server.
Everything downstream is unchanged.

> **Written, but never run against a cluster.** There is no multi-node Valkey in the test setup, so the
> whole cluster path — routing, `MOVED` handling, script registration across masters, a blocking `XREAD` on a cluster
> connection — is covered by reasoning about Lettuce's API and by nothing else. The standalone path is the
> one the 18 tests exercise. Cluster mode is exercised by the compose fixture below — the standalone e2e
> suite runs unchanged against a real three-node cluster.

> **The defect this fixture exists to never readmit (found in review, 2026-09-11).** The listener used to
> read every wake stream in one `XREAD`, and Redis Cluster rejects a multi-key read across slots — so in
> cluster mode every read failed and the wake system silently degraded into announce-all polling on the
> retry backoff. It survived three PRs because standalone Redis has no slots: no test that ran could see
> it. Streams are now grouped **by slot** when the store is a cluster — one reader connection per group,
> opened at startup, one listener fiber on each, collapsing to a single reader on a server that has no
> slots — and the claim is tested the only honest way:
>
> ```bash
> DKQ_E2E_STACK=cluster sbt e2e
> ```
>
> runs the whole e2e suite over a real three-node cluster, composed inside the docker network (a
> containerized cluster announces container addresses, so only another container can follow them).

## Why the key layout was ready first

Every key a queue owns carries the hash tag of the **partition** that queue falls in. `QueueKeys` builds all
ten from `prefix = "{p:<partition>}:v3:q:<queue>"`, with the wake stream tagged but not scoped to the queue:

```
{p:0}:v3:q:orders:ready      {p:0}:v3:q:orders:fence       {p:0}:v3:q:orders:msgs:<key>
{p:0}:v3:q:orders:seq        {p:0}:v3:q:orders:attempts    {p:0}:v3:q:orders:payloads:<key>
{p:0}:v3:q:orders:claimed    {p:0}:v3:q:orders:delayed     {p:0}:v3:q:orders:owned:<key>
                             {p:0}:v3:wake                 ← shared by every queue in partition 0
```

`partition = hash(queue) % 16` — the count is a constant of the code, not a deployment parameter. What each
structure is for is [`redis-data-structures.md`](redis-data-structures.md); what matters here is only that
everything a script touches carries the same tag.

**The `v3` is the schema version, and it sits outside the tag on purpose.** Only what is inside the braces
is hashed, so the segment moves nothing between slots — and a key's incarnations under two schemas land in
the *same* slot, which is what would let a migration step read v1 and write v2 in one script
([`../research/schema-versioned-keys.md`](../research/schema-versioned-keys.md)).

**The locks are partitioned the same way, and share the partition** — `{p:<partition>}:v3:l:…`. A lock's
partition follows its name, so every operation on one lock reaches the same slot, and each script touches
that partition's leases, tokens, fence counter and waiters list together.

Sharing the tag means sharing the wake stream, which is the point: a stream costs a slot and therefore a
blocking connection, and two tag spaces cost two of each for no gain. What used to make them separate was
that a stream fed exactly one waker — a queue's readiness hands one token to one consumer, a lock's wakes
everyone — so a shared stream would have sent lock wakes to the queue's sink. That is now decided by the
entry rather than the stream: each carries a `kind` field (`q` or `l`) and the listener routes on it, so
one stream feeds both sinks without either seeing the other's wakes.

The fence counter is therefore one per partition rather than one for the deployment, which is safe for the
reason the global counter was: a fence need only increase within a single lock, and a lock never changes
partition.

Nothing says which slot a given tag lands in, which is why the streams are grouped by *computed* slot
rather than by kind or by partition. What may be grouped is `KeyLayout`'s to say — it is the one thing that
knows both the partition count and whether the store is a cluster, and the only part of it allowed to know
the latter, since a key's own partition must come out the same on either deployment. `Connection` opens one
connection per partition, at startup, and the listener runs a fiber on each, reading that partition's one
wake stream.

A partition's keys hash to one slot, and **every script touches exactly one queue, whose keys are all in its
partition's slot** — which is what makes the Lua legal at all, since a script may only reach keys in a single
slot. The wake stream is in that slot too, which is the whole reason the tag is the partition rather than the
queue: a stream tagged differently from the keys it announces could not be appended by the script that made
them claimable, and a separate append is a crash window where work exists and nobody is told.

**The partition count is the deployment's, and it is one of two values.** A cluster uses sixteen; a single
server uses one. Partitions exist to spread keys across slots, and a server that has none has nothing to
spread across — so on one the sixteen would buy nothing and cost a blocked connection each, while on a
cluster the count is a ceiling on spread (sixteen nodes at the very most, far above any realistic cluster
for one service) against a floor on overhead (sixteen mostly-idle streams, and the connection each one's
slot needs).

It is not a knob beyond that: an operator picks a store, not a count. And because the count is written into
every key as its tag, an instance that disagrees about it would read and write different names entirely —
which is why it is recorded in the marker below rather than left to configuration to get right.
[`../research/bucketed-wake-streams.md`](../research/bucketed-wake-streams.md) has the original reasoning
and what partitioning cost.

**The store records the schema it was written under, and instances refuse to disagree.** At first boot each
instance records its layout — the schema version and the partition count, as `v3.p16` (`dkq:layout:schema`,
claimed with `SET NX` so racing first boots cannot both write); every later boot compares and **refuses to
start on a mismatch**, before anything is served — a rolling deploy of incompatible code crash-loops loudly instead of misreading live structures.
The version is bumped in code whenever an older instance would misread the store: a structure changing
type, an encoding changing form. The partition count needs no bump — it is in the marker itself. Gate-only, never migrated: the remedy is a
ceremony, in this order — **stop every instance**, drain dkq (no queued work, no outstanding receipts or
holds; delete dkq's keys, or flush the store **only if it is dkq's alone** — an existing shared Redis is a
supported home, and its other tenants are not dkq's to flush), **delete the marker** (`DEL
dkq:layout:schema`), then start instances: the first one records the new layout as it boots.

The whole ceremony is on the operator, and nothing in the service can check it. Deleting the marker says
"what is in this store is now yours" — it cannot verify the drain, because what a leftover key looks like
depends on the layout being replaced, which the new code no longer knows; and it cannot see instances,
which check their layout at boot and never again. What no store-side marker covers at all is client-held
state such as receipts: a receipt-format change breaks holds the store never sees.

Two consequences are easy to undo by accident:

- **`claim.lua` and `sweep.lua` build key names at runtime** — `prefix .. ':msgs:' .. key` and its
  siblings — without declaring them in `KEYS`. Reaching an undeclared key is only safe because the tag
  guarantees the same slot. That is why both take `prefix` as an argument at all, and it is unavoidable in
  `claim.lua`, which does not know which key it holds until it has popped one.
- **The `wake` stream carries the partition's tag for the same reason.** A stream tagged differently from the
  keys it announces would be a different slot, so
  the entry could not be appended by the script that made the key claimable — and a separate append is a
  crash window where work exists and nobody is told.
- **`renew` groups claims by queue** and issues one call per queue rather than one for a caller's whole
  receipt set. A single call across two queues is a call across two tags, and therefore two slots.

Neither is visible as a failure on a single node, so both would break cluster mode silently.

## What the two backends share, and where they differ

`Connection.Commands` is bound by `RedisClusterCommands`, which Lettuce makes the supertype of both:
`RedisCommands extends RedisClusterCommands`, and so does `RedisAdvancedClusterCommands`. Every command
this adapter uses — `evalsha`, `scriptLoad`, `blmove`, `lmove`, and `zcard` in the specs — is on it.

What the narrower `RedisCommands` bound would have added is `RedisTransactionalCommands`: `MULTI` / `EXEC`.
This adapter must never use those anyway — every operation is exactly one script, so that there are no
interleavings to reason about — so **the bound turns that rule into something the compiler keeps.**

They differ in exactly two places:

1. **`Connection`** has a `cluster` client and an `open` for it, beside the standalone pair. `pool` takes an
   opener rather than a client, so it serves both without knowing which it has.
2. **`LuaScript.loadEverywhere`** registers the script on every master. `RedisAdvancedClusterCommands`
   overrides the cluster-wide script commands — `SCRIPT FLUSH`, `SCRIPT KILL` — but **inherits `scriptLoad`
   unchanged**, so on a cluster connection it would still reach one node. With no `NOSCRIPT` fallback (see
   `Scripts`), a call routed anywhere else would simply fail. Broadcasting is the node-selection API,
   `upstream().commands().scriptLoad(bytes)`; every master returns the same digest, since a digest is a hash
   of the script.

The choice is made inside `Module.connection` rather than by a separate layer, because a layer cannot pick
its own inputs and Lettuce has no URL scheme that distinguishes the two.

## What sharding buys — and what it does not

**A queue is one slot, so a queue is one node.** Sharding spreads *queues* across a cluster; it can never
spread a single queue. That is a property of the hash tag, not a gap: the tag is what lets a script be
atomic across a queue's keys at all, and `docs/research/redis-keyed-queue.md` records the trade — atomicity
and reliability intact, at the cost of no sharding within a queue.

So cluster mode is worth having for **many queues**, or for failover. It does nothing for one hot queue. If
a single queue ever outgrows a node the answer is a different key layout, not a bigger cluster — and on
`docs/research/throughput-first-numbers.md` that is a long way off.

## What would make it trustworthy

A three-node Valkey cluster in `QueueStoreSpec` or the compose file, running the existing suite unchanged.
The claims most worth testing rather than reasoning about:

- an `XREAD` parked on a cluster connection blocks only that connection, as it does standalone — this is
  the assumption `Connection.Pool` rests on, and the one least supported by reading the API;
- `Scripts.make` leaves every master able to serve `EVALSHA`;
- the watchdog's runtime-built keys resolve, which is the check that the hash tag is doing its job.
