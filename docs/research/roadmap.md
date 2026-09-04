---
title: "From POC to implementation — a working order, and why it is in this order"
type: research
status: draft
updated: 2026-08-30
tags: [roadmap, sequencing, connection, streaming, persistence, distributed, exploration]
---

# From POC to implementation

> **This order will change, and that is fine.** It is a working sequence, not a commitment: each phase is
> here because of what it *locks in*, and when a phase teaches something that invalidates the reasoning, the
> order should move. What this note is really for is recording the *reasons*, so that a reordering is a
> decision someone made rather than drift nobody noticed. Expect the exit criteria to survive longer than the
> sequence.

Companion note: [`redis-keyed-queue.md`](./redis-keyed-queue.md) for the substrate design. The
requirement all of this serves — per-key serial processing with long-lived handlers, concurrency across
keys — is summarised in the [README](../../README.md).

## The order

### 1. Make it work on the most convenient substrate (Redis)

**Goal:** enqueue, blocking dequeue, per-key serial processing, concurrency across keys — end to end, over
gRPC, across more than one pod.

**Locks in:** the state machine (`idle` / `queued` / `processing`), the lease-and-heartbeat model, the
fencing token, and the shape of the message.

**Must not lock in:** anything Redis-specific above the port boundary. A claim script and a deadline ZSET —
`BLMOVE` and a deadline ZSET, when this was written — are one implementation of "claim a ready key, with a
deadline"; the phase fails its own purpose if that leaks into the API.

**Done when:** two pods, one Redis, a client whose dequeue on pod A is woken by an enqueue on pod B, and a
killed worker whose key is reclaimed and reprocessed.

**The API this phase ships** is sketched in [`phase-1-api.md`](./phase-1-api.md): four unary calls —
`Enqueue`, a blocking `Dequeue`, `Settle`, `Extend` — written to be replaced by phase 2 rather than extended
into it.

**Deployment shape — dkq is stateless here, and that is the point.** Every piece of state (queues, keys,
leases, in-flight messages) lives in Redis, so dkq pods are interchangeable: a plain `Deployment` with N
replicas, no PVC, no stable identity, scaled at will.

The Redis goes *inside the app*, not in `infrastructure/`. A POC instance with no persistence and exactly one
consumer is dkq's private dependency, not shared homelab infrastructure — the cluster repo draws that line
already (`infrastructure/postgres` is shared and `prune: false`; `apps/<name>/` is a service's own
manifests). So:

```
apps/dkq/
  deployment.yaml         dkq, N replicas, stateless
  service.yaml            ClusterIP for gRPC
  redis.deployment.yaml   one Valkey pod, emptyDir
  redis.service.yaml      ClusterIP :6379   → dkq reaches it at redis:6379
```

Three things that get cheaper this way: no ESO/Reflector mirroring (the password Secret shares a namespace
with its only consumer), `prune` can stay **on** (an `emptyDir` instance has nothing to lose, unlike
`infrastructure/postgres`), and the shared-instance questions — ACL user per service, key prefixes — are
deferred until a second service wants a queue. `docker-compose.yml` in this repo mirrors it for local work.

The cost, stated plainly: **a Redis pod restart loses every queued job**, including an ordinary node drain or
image bump. Acceptable while proving semantics; unacceptable the moment anything real depends on it. That day
is phase 3, and it is also when the instance probably moves to `infrastructure/` with a PVC,
`strategy: Recreate`, `prune: false`, and the ESO pipeline.

### 2. Streaming API with credits

**Goal:** replace unary long-poll with a server stream where the client declares capacity ("I can take 4
more").

**Why here and not later:** this is the wire contract, not an optimisation. It changes what every client
writes, how cancellation behaves, and how a lease is scoped — a dropped stream reclaims everything that
stream held, rather than one call's worth. Settle it while the substrate is a Redis that can be wiped, not
after a distributed core has grown on top of it.

**The property it buys for free:** credits are *addressed* demand. They belong to one stream and die with the
connection, which is the guarantee anonymous demand tokens could not give — the toolkit's `PollConsumer`
([https://github.com/AndreMeira/homelab-toolkit-zio](https://github.com/AndreMeira/homelab-toolkit-zio)) spent a day learning why that distinction matters, when demand outlived the workers
it was issued for. "Outstanding demand equals available workers" becomes a
property of the connection lifecycle rather than something to reconcile after the fact.

**Done when:** a worker holds one stream for its lifetime, backpressure is expressed as credits rather than
as repeated polling, and killing the stream releases its keys promptly.

### 3. Persistence as a second adapter

**Goal:** the same state machine over Postgres — `SKIP LOCKED` and a `claimed_until` column doing what the
claim script and the deadline ZSET do.

**Why it is not a phase of its own:** if the port boundary held in phase 1, this is writing an adapter and
running the existing tests against it. If it turns out to be a redesign, that is the phase-1 boundary
failing, and better to learn it here than after phase 4.

**Done when:** the conformance suite passes unchanged against both substrates, and a Postgres failover loses
no acknowledged message.

### 4. Shared-nothing: in-memory state with peer-to-peer coordination

**Goal:** no external store — state lives in the pods, keys are owned by nodes, and ownership moves as pods
come and go.

**Why last:** it is the largest step by a wide margin — membership, partition ownership, routing, rebalancing
and failure detection — and it is the part where a subtle mistake stays invisible longest. It also *reduces*
what the system guarantees until persistence is layered back in, so doing it early means taking the biggest
complexity jump to arrive at something trusted less.

**The honest caveat:** if the real goal is to learn distributed systems rather than to run a queue, this phase
*is* the deliverable and the rest is scaffolding — in which case do it early, but on a branch, with the
conformance suite as the arbiter. Worth deciding which it is before starting, because the answer reorders
everything above.

**Done when:** a rolling restart moves ownership without losing per-key ordering, and the conformance suite
still passes.

**It also flips dkq's own workload shape**, which is a fair proxy for the size of the phase. Stateless
`Deployment` becomes `StatefulSet` plus a headless Service, because peers must address each other by name
(`dkq-0.dkq.svc`) for ownership and membership to mean anything — and with persistence, a PVC per pod. That
is the same kind of one-way door as "shared store → shared nothing", and worth counting as part of the
phase's price rather than as an implementation detail discovered later.

## Cross-cutting, and cheap only if started early

- **A substrate-agnostic conformance suite**, written during phase 1: per-key order preserved; never two
  *effective* workers on one key; nothing lost when a worker is killed mid-handler; a poison message does not
  wedge its key forever. Every later phase must pass it unchanged. This one artefact is what turns "swap the
  substrate" from a hope into a claim.

  Two of those four now mean something narrower than when this was written, because a claim covers a
  **batch** of a key's messages rather than one. "Never two workers on one key" is still enforced — the key
  is what is owned — but *within* a batch, working one message at a time is the consumer's discipline rather
  than something the store can refuse. And "per-key order" now means the order messages are handed out: a
  nacked message is retried after later ones a consumer chose to process, because a nack no longer blocks
  what is behind it. That was a deliberate trade
  ([`look-ahead-and-discard.md`](look-ahead-and-discard.md)), and the suite should assert the guarantee that
  was actually chosen rather than the one this line originally described.
- **Dead-letter and attempt limits.** API-visible, and *no longer urgent*: a nacked message stays where it
  is and later messages for its key are handed out past it, so a permanently failing message cycles rather
  than wedging its key. `attempts` is per message id and climbs on every redelivery, so the signal a policy
  would read is already there — what is missing is somewhere to put a message once the count is too high.
- **Observability** via `homelab-telemetry`: queue depth, age of the oldest ready message, reclaim rate,
  fencing-token rejections. Reclaim rate is the number that says whether the lease and heartbeat are tuned
  sanely; oldest-ready age is the one that catches every missed-wake-up bug at once, whatever its cause.

## What would justify reordering

- Phase 1 shows the message or the state machine is wrong → fix it before phase 2, since phase 2 freezes the
  contract.
- Phase 2 shows credits do not compose with per-key exclusivity (a worker holding credits for keys it cannot
  be given) → that is a design problem, not a sequencing one, and it stops the line.
- Redis durability turns out to be acceptable for every real workload → phase 3 becomes optional rather than
  inevitable.
- The learning goal wins over the running goal → phase 4 moves to the front, on a branch.
