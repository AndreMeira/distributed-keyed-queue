---
title: "Where dequeue latency goes, and what would actually move it"
type: research
status: draft
updated: 2026-09-05
tags: [latency, design, redis, grpc, ownership, leases, exploration]
---

# Where dequeue latency goes

Not built. An exploration of what a message actually waits for between being enqueued and reaching a
consumer, and which changes would move that number rather than decorate it. The measurements are real; the
designs are not.

## The budget, measured

Nothing here is compute-bound, and it is worth seeing by how much.

| | measured | how |
|---|---|---|
| Redis executing `produce.lua` | **4.18µs** (p99 16µs) | `valkey-benchmark`, 20,000 `EVALSHA`, `INFO commandstats` |
| a round trip to Redis from the same container | **55µs**, 18,570 rps on one connection | same run |
| ZIO's blocking-pool hop | **0.3µs** | 200,000 iterations against `ZIO.succeed` |
| an enqueue, end to end | **6–7ms** | `ThroughputSpec` indicator |
| a message reaching an idle consumer | **~10ms** | same |

So Redis is **0.06%** of an observed request and the effect-system ceremony is 0.005%. The rest is
transport — and in that harness, chiefly gRPC crossing Docker Desktop's host-to-VM boundary. Throughput says
the same thing from the other side: **~4,000 msg/s at ~7ms means about 28 requests in flight**, not seven
milliseconds of work each.

The details of the first three rows, and why the synchronous Redis client is fine, are in
[`../architecture/redis-connections.md`](../architecture/redis-connections.md).

## The hop model

Every remaining millisecond is a network round trip, so the only lever is removing hops or shortening them.
A dequeue that had to wait costs, on a sane network (estimated, not measured):

```
enqueue  client → dkq → Redis → back           ~0.5ms   2 RTT
notify   XADD → the listener's XREAD returns   ~0.2ms   1 RTT, already pushed
claim    consumer → dkq → Redis → back         ~0.5ms   2 RTT
                                               ──────
                                               ~1.2ms, of which ~10µs is real work
```

**The claim round trip is the interesting one.** It exists because a wake says "look again" rather than
"here is your work" — the store already knew a consumer was waiting, and made it ask anyway. Every strategy
below is a different answer to that, and they divide into two families: keep the round trip but take it off
the critical path, or remove it by moving where work is decided.

## Two regimes, and they have different answers

The strategies below do not all help the same case, and conflating the two is the easiest way to build the
wrong one. Writing `c` for one-way client↔dkq and `r` for one-way dkq↔Redis, both roughly 0.2ms on a sane
network:

| | today | credits + prefetch | granting at enqueue |
|---|---|---|---|
| **idle** — empty queue, a message arrives | `2c + 4r` ≈ 1.2ms | ≈ 1.2ms, no gain | `2c + 2r` ≈ 0.8ms |
| **backlog** — work waiting, a consumer frees up | `2c + 2r` ≈ 0.8ms | ≈ `c` ≈ 0.2ms | ≈ 0.8ms |

**Prefetch cannot help the idle path**, and not by accident: there is nothing to fetch ahead of. When the
queue is empty the instance must still learn a message arrived and still claim it. Only deciding *at enqueue
time* who gets the work removes that round trip.

**Prefetch is worth the most on the busy path**, where it removes the whole ask-and-wait cycle: the next
claim is already in the instance when a consumer finishes, and with a stream the consumer does not even send
a request. Per-consumer ceiling goes from roughly `1/(2c+2r)` ≈ 600 msg/s to several thousand — a throughput
result wearing a latency costume.

Keep both next to the number that dwarfs them: **the deployment is worth ~5ms**, since today's 6–7ms against
an estimated 1.2ms is Docker Desktop's host-to-VM boundary. Everything below argues over the last
millisecond.

## Step 1: credits over a stream

**No ownership, no partitioning, no substrate change.** A credit is satisfied by *any* key that becomes
claimable, so this touches the protocol and the server only — the claim script, leases, fences and the
exclusivity guarantee are untouched, which makes it reversible and measurable on its own.

**Credits are pull semantics with push latency.** The consumer says "I can take 100" and the server may
deliver up to a hundred without being asked again, but **never more than the outstanding credit**. Polling is
the same protocol with the credit pinned at 1 and re-issued synchronously, which is exactly why it costs a
round trip per unit of work. Reactive Streams calls it `request(n)`, AMQP calls it `basic.qos`, JetStream
calls it a pull batch. The backpressure property that makes polling attractive is kept; only the demand
becomes explicit.

**A filled credit is a lock.** In an ordinary broker a prefetched message sits in a buffer and costs memory.
Here it is a claim — a key held exclusively, lease ticking, that nobody else may work. Over-crediting is
therefore harmful rather than merely wasteful: a consumer that credits 100 while it can run ten handlers
holds ninety keys idle toward expiry while its peers starve.

That fixes the unit. **A credit is a concurrent handler**, which turns C7 from advice into something the
protocol enforces — thirty-two worker fibers means thirty-two credits and structurally no thirty-third key.
An *unfilled* credit costs nothing; it is permission, not a lock.

**The stream is the registration.** Any design that delivers work unasked needs to know who is waiting, and
the obvious version of that is a registry in Redis with patience deadlines, so a script never grants to a
phantom. A bidirectional stream is that registration with exactly the right lifetime by construction:
credits live in the connection and die with it. It also collapses `Dequeue`, `Settle` and `Heartbeat` into
one stream, and stream liveness beats a periodic heartbeat — a dropped stream means the consumer is gone
*now*, so its claims can be revoked at once rather than after a lease TTL.

**Prefetch is the server-side half.** The instance knows its local credit total, so it claims ahead and holds
the results for whichever credited consumer takes the next one. The Redis round trip still happens; it
happens *off the critical path*, and the consumer's latency becomes a local handoff of microseconds.

**Batch the claims, because that is where the round trips go.** `consume.lua` claims one key per call today.
With credits the instance knows it wants `n`, so `ZPOPMIN ready COUNT n` grants them in one script: per-key
cost falls from a round trip to a round trip ÷ `n` plus ~4µs of Lua. At `n = 32` that is ~6µs of network per
key, and Redis round trips stop bounding throughput at all.

**And credits are what make batching free.** Every batching scheme normally needs a knob and a timer —
Kafka's `linger.ms`, Nagle's algorithm — because the server must decide how long to *wait* for a batch to
fill, which is a straight trade of latency for throughput. Credits remove the decision: the instance already
knows `n`, because `n` is demand that has been declared. There is nothing to accumulate and nothing to wait
for, so batching costs no latency at all. It is the difference between "hold this back in the hope more
arrives" and "take as much as is already wanted".

That also disposes of a tuning parameter rather than adding one, which is the opposite of how batching
usually arrives.

`n` must be capped, for the reason `watchdog.lua` already takes a `limit`: **a script blocks the whole
server.** Thirty-two keys of thirty-two messages is ~130µs of Lua and harmless; a thousand would be a
multi-millisecond stall for every other client. The cap is a latency decision, not a throughput one.

**Settles want the same treatment.** The two round trips per message are the claim and the settle, so
batching only the first leaves half the win. `complete.lua` is per-claim today; the counterpart takes
several `(key, token, outcomes)` triples. Heartbeats already batch per queue, so the pattern exists.

**Two things it fixes inside the instance**, incidentally:

- **The herd goes, without the broadcast going.** Today a wake reaches every parked consumer and they all
  attempt. With credits the *instance* holds the demand: one claimer per instance instead of N, batched.
- **`Waiters` gets simpler.** The signal stops meaning "wake a consumer so it can ask" and starts meaning
  "there may be work; fill credits". The consumer-facing handover — the thing that caused all the
  interruption trouble — disappears.

**What it costs:**

- **Speculatively claimed keys are locked before anyone works them.** Bound credits by concurrency; the
  lease is the backstop.
- **A claim fetched but never delivered must be released promptly**, not left to expire — the orphan-nack
  rule from `PollConsumer`: every claimed job gets a worker or a nack.
- **Batch size interacts with the lease.** `n` keys claimed together start `n` leases at the same instant, so
  the last one worked has had its lease running longest. Over-batching surfaces as spurious reclaims rather
  than as slowness.
- **Hoarding**, if credits are not bounded by concurrency. Filling round-robin across streams with
  outstanding demand is the answer, and it is now a policy the server can choose, because it knows the
  demand.

## Step 2: slice leases

Per-key exclusivity needs an authority that decides who holds `k`, and **the latency floor is the distance to
it**. There are three places to put it:

| authority | round trips to claim | ownership | cost |
|---|---|---|---|
| in the store, per key (**today**) | 1 per claim | fully dynamic | a round trip every time |
| in the store, leased per **slice** | 1 per lease, then none | dynamic, coarse | rebalancing |
| pinned to consumers (**Kafka**) | 0 | static until rebalance | a slow key blocks its partition |

Partition each queue's keyspace into slices — the same arithmetic as buckets — and let a consumer **lease a
slice**: one owner, fenced, expiring, exactly like a key claim but wider. While it holds the slice it may
work any key in it without asking anyone, because nobody else may touch them; exclusivity becomes a local
lock. Pair it with delivery and steady state is one hop for the enqueue and one for the push, an estimated
**~0.4ms against ~1.2ms**.

**A slice is a unit of ownership, not of sequencing** — which is what makes it better than the Kafka cell
rather than a copy of it. The holder runs every key in its slice concurrently and serialises only within a
key, so a slow key blocks its own key and nothing else. The obvious objection — a hot key can then only be
worked by one consumer — costs nothing, because per-key exclusivity already meant exactly that.

**It also makes granting at enqueue free**, which is why it belongs after step 1 rather than beside it. The
expensive part of delivering work unasked was knowing who should get it. With slices *ownership is already
recorded*: the enqueue script looks up who holds the key's slice and delivers there. No demand registry, no
patience deadlines, no phantom grants — and that is what closes the idle path, the one thing step 1 cannot
touch.

**The cost is rebalancing, and it is the whole budget.** A slice held for `T` seconds cannot be helped by an
idle peer, so it wants more slices than consumers, a lease short enough to redistribute, and a release
protocol — a waiting consumer asks, the holder yields at its next safe point. That protocol is what makes
this a rewrite rather than a change. Failover generalises rather than multiplies: lease and fence machinery
already exists, one level up, and messages never move, so nothing needs repairing.

**Slices and buckets want to be the same number** — one partition serving as cluster slot, delivery channel
and ownership unit. That is where dkq stops being a stateless shell over Redis and becomes a partitioned log
with per-key concurrency inside each partition.

Not to be confused with the named claim in
[`claimable-keys-as-a-set.md`](claimable-keys-as-a-set.md), which keeps the authority in the store and so
still pays a round trip per claim; what that buys is less *contention*. One is a week's work, the other is a
rewrite.

## The expensive way to get step 2's idle-path win

Granting at enqueue time can be built *without* ownership: keep a registry of waiting consumers in Redis,
scored by patience deadline so the script never grants to a phantom, and have `produce.lua` pop one and
grant the claim outright — lease, fence, `owned` — delivering the grant instead of a hint.

**It breaks "one signal wakes everybody", and that is allowed.** The broadcast signal exists because a wake
lives only in one process's memory: lose one and nothing can notice, so the design made it impossible to
lose. A grant is a claim in Redis, so one nobody collects lapses and the watchdog reclaims it — losing it
costs bounded latency, not correctness.

> **The principle:** broadcast when the thing handed over is only in memory; address it when the store can
> recover it. See
> [`../learning-material/interruption-and-lost-wakes.md`](../learning-material/interruption-and-lost-wakes.md).

It is not even a new failure mode: a consumer that dies just after claiming already strands its key until the
lease expires, and a grant strands the same way with a window of one network hop rather than a whole handler.

The shape that costs least reuses the existing listener — wake entries gain an optional `to` field, so an
addressed grant travels the bucket streams every instance already reads, needing no new streams and no
dynamic subscription.

**Build this only if the idle path matters and ownership is unwanted.** It buys ~0.4ms for a registration
lifecycle in the store; step 2 gives the same thing for free, having paid for ownership already.

## Colocating the state

The extreme of step 2: the store lives in the process, partitioned by key with one owner per key, and the
dkq-to-Redis hop disappears entirely — twice on the wake path. An enqueue becomes one client round trip, a
local mutation, and a handoff to a locally parked consumer: plausibly **~100–200µs**.

What makes it thinkable is that **the POC already runs Valkey with saving off**, so total loss on crash is
within the current contract — replication is optional, and the consensus cost that usually kills this idea
goes with it. What you buy instead is routing, and failover with fencing so a partition cannot be owned
twice. Honestly framed: writing a small piece of Kafka, in exchange for the per-key serial handlers Kafka
cannot give.

## Not worth it

- **A faster store.** 4µs is not the problem, and nothing can improve it meaningfully.
- **KeyDB, Dragonfly.** Multithreading raises throughput; single-operation latency is already microseconds.
- **RESP3 push instead of `XREAD BLOCK`.** Already a push once established; no hop is removed.
- **An async Redis client.** Measured at 0.3µs per hop — see
  [`../architecture/redis-connections.md`](../architecture/redis-connections.md). A thread-count change, not
  a latency one.

## Order of work

1. **Measure on one host.** Everything here assumes the transport model is right, and the deployment is
   worth more than every item below combined.
2. **Step 1 — credits, prefetch and batching.** Most of the throughput win, no substrate change,
   backpressure preserved and made explicit. Stands on its own even if nothing follows.
3. **Step 2 — slice leases**, if the idle path matters after step 1. It also delivers granting-at-enqueue as
   a side effect, which is the only thing that shortens that path.
4. **Colocating the state**, only if a workload appears that cares about the difference between 1.2ms and
   0.2ms. None does yet.

The ordering has a bias worth naming: step 1 changes the protocol and the server, step 2 changes the
substrate contract. Protocol changes are cheap to reverse and easy to measure in isolation; substrate
changes are neither. When two designs buy similar amounts, take the one that does not touch what Redis holds.
