---
title: "Redis Cluster — the key layout is ready, the client is not"
type: architecture
status: current
updated: 2026-08-30
tags: [redis, cluster, sharding, hash-tag, lettuce, lua]
---

# Redis Cluster

**dkq does not run against a Redis Cluster today.** The blocker is the client, not the data model: the part
that is hard to retrofit — the key layout — was designed for cluster mode from the start, and the part that
is missing is confined to two files.

## What is already correct

Every key belongs to one queue and carries that queue's hash tag. `Namespace` builds all eleven of them from
`prefix = "{q:<queue>}"`:

```
{q:orders}:ready        {q:orders}:fence       {q:orders}:msgs:<key>
{q:orders}:state        {q:orders}:attempts    {q:orders}:inflight:<key>
{q:orders}:claimed      {q:orders}:workers     {q:orders}:claiming:<worker>
                        {q:orders}:delayed
```

So one queue's keys hash to one slot, and **every script touches exactly one queue**. That is what makes the
Lua legal in cluster mode, where a script may only reach keys in a single slot.

Two consequences of that are easy to miss and are load-bearing:

- **`watchdog.lua` builds key names at runtime** — `prefix .. ':inflight:' .. key`, `prefix .. ':claiming:'
  .. worker` — without declaring them in `KEYS`. Reaching an undeclared key is only safe because the tag
  guarantees the same slot. That is why the sweep takes `prefix` as an argument at all.
- **`renew` groups claims by queue** and issues one call per queue rather than one call for a caller's whole
  receipt set. A single call across two queues would be a call across two tags, and therefore two slots.

## What blocks it

**The client is standalone.** `Connection.client` is `RedisClient.create(uri)` and `Connection.open` is
`client.connect(codec)` — there is no `RedisClusterClient` in the repo. A standalone client does not follow
`MOVED`, so against a cluster it serves only the slots of the node it happened to connect to and fails on
everything else.

**Script registration reaches one node, and switching client does not fix it.** `LuaScript.register` calls
`scriptLoad`, which registers the digest on whichever node served the call. In a cluster the digest must
exist on every master, and `RedisAdvancedClusterCommands` does **not** broadcast it: it overrides
`scriptFlush`, `scriptKill` and `flushall` — the cluster-wide ones — but inherits `scriptLoad` unchanged, so
it still goes to a single node. Broadcasting is the node-selection API,
`masters().commands().scriptLoad(bytes)`, which returns `Executions[String]` rather than one digest.

This matters more here than in most designs because there is deliberately **no `NOSCRIPT` fallback** (see
`Scripts`): a call routed to a node that never received the script fails rather than recovering. It is also
the one piece of the work that does **not** live in `Connection`.

**Blocking needs a pinned connection.** `BLMOVE` must run on a connection bound to the node owning the slot.
`Connection.Pool` currently hands out plain `StatefulRedisConnection`s from the standalone client, so the
claiming path would need the cluster equivalent.

## What sharding would buy — and what it would not

**A queue is one slot, so a queue is one node.** Sharding spreads *queues* across a cluster; it can never
spread a single queue. That is a property of the hash tag, not a gap to close: the tag is what allows a
script to be atomic across a queue's keys at all, and `docs/research/redis-keyed-queue.md` records the
trade — atomicity and reliability intact, at the cost of no sharding within a queue.

So cluster support is worth having when there are **many queues** to spread, or for failover. It does
nothing for one hot queue. If a single queue ever outgrows a node the answer is a different key layout, not
a bigger cluster — and on the numbers in `docs/research/throughput-first-numbers.md` that is a long way off.

## What it would take

Two files, `Connection` and `LuaScript.register`:

1. **Widen the opaque type's bound.** `Connection.Commands` is `<: RedisCommands[String, Array[Byte]]`, and
   `RedisAdvancedClusterCommands` is not a `RedisCommands`. Lettuce supplies the common supertype already:
   `RedisCommands extends RedisClusterCommands`, and so does `RedisAdvancedClusterCommands`. Rebinding
   `Commands` to `RedisClusterCommands` covers both with no abstraction of our own — and every command this
   adapter uses (`evalsha`, `scriptLoad`, `blmove`, `lmove`, and `zcard` in the specs) is on it. The only
   thing `RedisCommands` adds and `RedisClusterCommands` lacks is `RedisTransactionalCommands`: `MULTI` /
   `EXEC`, which this design never uses because every operation is one script.
2. **A cluster branch in `Connection`.** `RedisClusterClient` in `client`, its connection in `open`. Nothing
   above the adapter names a connection type, so this stops at the package boundary.
3. **Broadcast `scriptLoad` in `LuaScript.register`**, via `masters().commands()`.
4. Check the claiming pool still receives node-pinned connections for `BLMOVE`.

`Namespace`, all five `*Script` classes, and every `.lua` file are untouched by all four. The specs type
their inspection connection as `RedisCommands` and would follow the same widening.
