---
title: "Redis Cluster — supported in code, unproven in practice"
type: architecture
status: current
updated: 2026-09-04
tags: [redis, cluster, sharding, hash-tag, lettuce, lua]
---

# Redis Cluster

Set `cluster = true` (or `DKQ_CLUSTER=true`) and `redis-url` is read as a seed node instead of a server.
Everything downstream is unchanged.

> **Written, but never run against a cluster.** There is no multi-node Valkey in the test setup, so the
> whole cluster path — routing, `MOVED` handling, script registration across masters, a blocking `XREAD` on a cluster
> connection — is covered by reasoning about Lettuce's API and by nothing else. The standalone path is the
> one the 18 tests exercise. Treat cluster mode as untested until a three-node fixture exists.

## Why the key layout was ready first

Every key belongs to one queue, and carries the hash tag of the **bucket** that queue falls in. `Namespace`
builds all ten from `prefix = "{w:<bucket>}:q:<queue>"`, with the wake stream tagged but not scoped to
the queue:

```
{w:0}:q:orders:ready      {w:0}:q:orders:fence      {w:0}:q:orders:msgs:<key>
{w:0}:q:orders:seq        {w:0}:q:orders:attempts   {w:0}:q:orders:payloads:<key>
{w:0}:q:orders:claimed    {w:0}:q:orders:delayed    {w:0}:q:orders:owned:<key>
                          {w:0}:wake                ← shared by every queue in bucket 0
```

`bucket = hash(queue) % DKQ_WAKE_BUCKETS`, fixed for the deployment's life. What each structure is for is
[`redis-data-structures.md`](redis-data-structures.md); what matters here is only that everything a script
touches carries the same tag.

A bucket's keys hash to one slot, and **every script touches exactly one queue, whose keys are all in its
bucket's slot** — which is what makes the Lua legal at all, since a script may only reach keys in a single
slot. The wake stream is in that slot too, which is the whole reason the tag is the bucket rather than the
queue: a stream tagged differently from the keys it announces could not be appended by the script that made
them claimable, and a separate append is a crash window where work exists and nobody is told.

**The bucket count is a permanent deployment parameter**, like a partition count. Changing it moves queues
between tags and strands whatever was written under the old one, so it is fixed at first use; changing it
means drain and restart. At one bucket the whole service is one slot — right for a single node, and no use
in a cluster. Above one, buckets spread across nodes and queues spread across buckets, which is where the
sharding actually happens: [`../research/bucketed-wake-streams.md`](../research/bucketed-wake-streams.md)
has the reasoning and what it cost.

Two consequences are easy to undo by accident:

- **`consume.lua` and `watchdog.lua` build key names at runtime** — `prefix .. ':msgs:' .. key` and its
  siblings — without declaring them in `KEYS`. Reaching an undeclared key is only safe because the tag
  guarantees the same slot. That is why both take `prefix` as an argument at all, and it is unavoidable in
  `consume.lua`, which does not know which key it holds until it has popped one.
- **The `wake` stream carries the bucket's tag for the same reason.** A stream tagged differently from the
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
