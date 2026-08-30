---
title: "Redis Cluster — supported in code, unproven in practice"
type: architecture
status: current
updated: 2026-08-30
tags: [redis, cluster, sharding, hash-tag, lettuce, lua]
---

# Redis Cluster

Set `cluster = true` (or `DKQ_CLUSTER=true`) and `redis-url` is read as a seed node instead of a server.
Everything downstream is unchanged.

> **Written, but never run against a cluster.** There is no multi-node Valkey in the test setup, so the
> whole cluster path — routing, `MOVED` handling, script registration across masters, `BLMOVE` on a cluster
> connection — is covered by reasoning about Lettuce's API and by nothing else. The standalone path is the
> one the 18 tests exercise. Treat cluster mode as untested until a three-node fixture exists.

## Why the key layout was ready first

Every key belongs to one queue and carries that queue's hash tag. `Namespace` builds all eleven from
`prefix = "{q:<queue>}"`:

```
{q:orders}:ready        {q:orders}:fence       {q:orders}:msgs:<key>
{q:orders}:state        {q:orders}:attempts    {q:orders}:payloads:<key>
{q:orders}:claimed      {q:orders}:workers     {q:orders}:owned:<key>
                        {q:orders}:delayed     {q:orders}:claiming:<worker>
```

What each of them is for is [`redis-data-structures.md`](redis-data-structures.md); what matters here is
only that they all carry the same tag.

One queue's keys hash to one slot, and **every script touches exactly one queue** — which is what makes the
Lua legal at all, since a script may only reach keys in a single slot.

Two consequences are easy to undo by accident:

- **`watchdog.lua` builds key names at runtime** — `prefix .. ':owned:' .. key`, `prefix .. ':claiming:'
  .. worker` — without declaring them in `KEYS`. Reaching an undeclared key is only safe because the tag
  guarantees the same slot. That is why the sweep takes `prefix` as an argument at all.
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

- a `BLMOVE` parked on a cluster connection blocks only that connection, as it does standalone — this is
  the assumption `Connection.Pool` rests on, and the one least supported by reading the API;
- `Scripts.make` leaves every master able to serve `EVALSHA`;
- the watchdog's runtime-built keys resolve, which is the check that the hash tag is doing its job.
