---
title: "Key stickiness — the same key to the same consumer, and knowing when that changes (pre-research)"
type: research
status: draft
updated: 2026-11-02
tags: [stickiness, affinity, assignment, rebalancing, consumer-identity, hooks, sharding, pre-research, not-a-decision]
---

# Key stickiness

> **Pre-research.** This note states the goal, what dkq already has that bears on it, the strategies worth
> exploring, and the questions each round of research has to answer. It decides nothing. This is the
> larger of the two features in [`../sessions/2026-11-02-next-features.md`](../sessions/2026-11-02-next-features.md),
> and several rounds are expected before a design.

## The goal

Two things, a little like partition assignment in a Kafka consumer group:

1. **Messages of one key go preferably to the same consumer pod.** A consumer that holds warm state for a
   key (a cache, a loaded aggregate, an agent's transcript) keeps getting that key, so it reloads less.
2. **A consumer can tell when it gains or loses a key**, or a group of keys. It gets hooks, or some other
   way to detect it, so it can load state on a gain and flush or drop it on a loss. Kafka's
   `onPartitionsAssigned` / `onPartitionsRevoked` is the reference point.

"Pod" here means a **consumer** pod, the process running the client. dkq instances are stateless and a
consumer reaches whichever one the load balancer picks, so stickiness is about which consumer gets a key,
not which dkq instance serves the call.

## The fact that shapes everything: exclusivity is already enforced

In Kafka the assignment *is* the exclusivity. A partition given to two consumers is processed twice, so
assignment has to be exact, and rebalancing has to stop the world (or be carefully cooperative) to stay
exact.

In dkq it is not. E1 ("at most one consumer is authorised to work a key at a time") is enforced by the
store on every claim, whoever asks. So **any stickiness scheme only has to be mostly right**:
- a key that goes to the "wrong" consumer costs a cold start, not a correctness bug;
- two consumers that briefly both think they own a key during a handover cost a contended claim, not a
  double processing.

The hooks inherit the same softness: a gain or loss is a hint about *where warm state is useful*, not a
grant of authority, which the claim and its fence remain. That is the main reason this can be cheaper than
Kafka's rebalancing, and every strategy below should be judged by how much of that cheapness it keeps.

## What dkq already has

- **No consumer identity.** R3 says recovery does not depend on one, and no request carries one. A claim
  is identified by its receipt, nothing else. Stickiness of any kind needs a consumer id, with a lifecycle
  (heartbeat, expiry). The `Heartbeat` RPC is a natural carrier, but not as the managed client uses it
  today: its `Heartbeat` beats only while it holds claims, at half the granted lease, and stops when the
  last one is settled. A consumer waiting for work would then look dead, and its keys would move for
  nothing. Membership needs a beat that also runs while the consumer is idle: a separate membership beat,
  or a heartbeat kept alive for the consumer's whole lifetime.
- **Accidental stickiness already exists.** [`guarantees.md`](../architecture/guarantees.md) notes that
  "the instance that just finished a key is often the one that takes it next", so a hot key tends to stay
  put. Nobody has measured how often. That is the baseline any design has to beat.
- **A named claim is cheap.** `ready` is a sorted set, so claiming a key by name is `ZREM`, O(log N). It
  was built that way for this ([`claimable-keys-as-a-set.md`](claimable-keys-as-a-set.md), step 3, not
  built). That note already splits the named claim into two flavours and names the fallback rule as a
  fairness decision.
- **Wakes are per queue, not per key.** `QueueReadiness` hands a token to *any* parked consumer and
  discards which key the wake was for. Routing a key to a particular consumer needs addressed wakes. The
  `to` field on a wake entry in [`dequeue-latency.md`](dequeue-latency.md) is one shape.
- **Slice leases were sketched as a latency idea** ([`dequeue-latency.md`](dequeue-latency.md), step 2).
  A consumer leases a slice of a queue's keyspace and owns every key in it. That is stickiness by
  construction, and the note is clear that rebalancing is "the whole budget".
- **The lock API.** It gives fenced, leased, queued ownership of a name, with refresh and release. It is a
  ready-made building block for owning a shard.

## Strategies to explore

Five shapes, from no server change to a large one. They can combine.

### A. Shard the queues themselves, in the client

> Worked out in detail, including how to share the shards fairly and where membership comes from, in
> [`lock-based-stickiness.md`](lock-based-stickiness.md).

The producer sends key `k` to `orders.<hash(k) % N>` instead of `orders`. Each consumer works some of the
N shard queues. This needs no server change: N queues are N ordinary queues, and a shard queue is a
different partition (`hash(queue) % 16`), so shards also spread across cluster slots.

Who works which shard is the real question:
- **Static.** Pod ordinal to shards, from a StatefulSet. Simple, but scaling means reassignment, and a pod
  that is down leaves its shards unworked.
- **Dynamic, through dkq's own lock API.** Each consumer try-acquires `orders.shard.<n>`, and holding the
  lock means working the shard. The hooks come with it: an acquire is a gain, and a refresh that fails or
  a lease that lapses is a loss. Spreading shards fairly is a policy on top: how many locks a consumer
  should hold, and when to give one up.

The costs:
- N is fixed by producers and consumers together, and changing it moves keys between shards. A key's old
  messages in `orders.3` and new ones in `orders.7` break O1 across the move unless the change drains
  first.
- Fewer shards than consumers leaves consumers idle.
- A shard queue that nobody owns is not worked. Whether to fall back to "anyone" after a grace period is a
  policy, not a given.

It could live entirely in the client library's managed layer, as a sharded producer and consumer. That
makes it the cheapest experiment, and a useful one even if a server-side design wins later.

### B. Soft affinity in the store: remember the last holder

The store keeps `{Q}:affinity`, key → the consumer id that last held it, written by the claim and expiring.
A claim from consumer `c` first takes a claimable key whose affinity is `c`, by name. Otherwise it takes
the head.

- Finding "a claimable key whose affinity is me" without scanning `ready` needs a per-consumer index, such
  as `{Q}:ready:<consumer>`.
- A key whose preferred consumer is busy or dead has to stay reachable by others. How long others hold off
  is the fairness knob.
- A loss can be *detected* exactly: when a claim grants a key whose affinity is another consumer, the
  script can append an "affinity moved" entry addressed to the old one. That is an `onLost(key)` hook
  delivered at the moment it happens.
- The authority stays in the store, so there is still one round trip per claim. It is a reduction in
  contention, not in latency.

### C. Hash keys over the live consumers

Consumers register in a membership set (`{Q}:members`, id → heartbeat deadline). A key's preferred
consumer is computed from the key and the members, for example by rendezvous hashing. A membership change
moves only about 1/n of the keys. Claims follow the hash, with a grace period after which anyone may steal.

- Gains and losses are computable from membership changes. Keys are unbounded, though, so the hook would
  be "your share changed", and the per-key gain or loss shows up lazily, at the next claim.
- Pulsar's `Key_Shared` subscription is close to this: per-key ordering across several consumers, with
  hash ranges assigned to consumers, sticky and consistent-hashing modes (from memory; to verify).

### D. Lease slices of the keyspace

The [`dequeue-latency.md`](dequeue-latency.md) step 2 design. Each queue has S slices. A consumer leases
slices, fenced and expiring, and claims only keys in its slices. The enqueue side can then deliver
straight to the slice's owner.

- The strongest stickiness, and the natural place for hooks: `onAssigned(slices)` / `onRevoked(slices)`.
- It also removes a round trip per claim, which A–C do not.
- It brings membership, rebalancing, a release protocol (a waiting consumer asks, the holder yields at a
  safe point) and starvation behind a slow owner. The note calls it a rewrite, and it is.
- The Kinesis Client Library's lease table (leases in DynamoDB, with lease stealing) and Akka Cluster
  Sharding (shard allocation, rebalance, hand-off) are the closest prior art (from memory; to verify).

### E. Sessions: the key is the unit, accepted and kept

Azure Service Bus sessions: a consumer *accepts* a session (a key), holds a lock on it, receives its
messages until it releases it or goes idle, and nobody else gets that key meanwhile (from memory; to
verify). In dkq terms, a claim that does not end when its batch is settled, but stays with the consumer
until an idle timeout. It is sticky for exactly as long as the key is busy, and gains and losses are
explicit (accept and release).

The cost is that it is a change to the claim's lifecycle, which the guarantees are written against.

## Questions every round has to answer

- **The consumer's identity.** Per process, or per pod name? A pod name that survives a restart is
  Kafka's static membership (`group.instance.id`). It is what stops a rolling deploy from reassigning
  everything twice.
- **The strength.** A preference with a grace period, or an assignment? Given E1, the leaning is a
  preference, unless D's latency is the goal.
- **What "a key" is for a hook.** Keys are unbounded and mostly idle. A hook per key (B, E) is exact but
  could be chatty. A hook per shard or slice (A, D) is bounded but coarse. C sits in between.
- **Fairness and starvation.** How long a key may wait for its preferred consumer before anyone takes it,
  and what a slow consumer does to its keys.
- **Interaction with what exists:**
  - batches (`max_batch`);
  - a nack's `retry_after` (`delayed`);
  - the watchdog's reclaim, which is a loss nobody chose;
  - the readiness token, which is any-consumer today;
  - the client's managed consumer (`Processor`, `.parallel`).
- **Scaling and deploys.** What one pod joining or leaving costs, and what a full rolling restart costs.
- **How to measure it.** A stickiness rate: the share of claims that go to the key's previous holder.
  First on today's accidental stickiness, then on every candidate, with the e2e throughput setup.

## Research rounds

1. **Baseline.** Measure today's stickiness rate on the e2e stack, under a few key and consumer shapes.
   If it is already high for hot keys, the problem is the cold and lukewarm keys, which changes what to
   build.
2. **Prior art, read properly:**
   - Kafka's assignors (range, round-robin, sticky, cooperative-sticky), static membership and the
     rebalance listener;
   - Pulsar's `Key_Shared`;
   - Azure Service Bus sessions;
   - the KCL lease table;
   - Akka Cluster Sharding;
   - the RabbitMQ consistent-hash exchange with single active consumer.

   For each: what is sticky, what moves on a change, what the hooks are, and what breaks under a slow
   consumer.
3. **Prototype A in the client.** Shard queues plus shard ownership through the lock API. No server
   change, and it tests the hooks' shape against a real consumer.
4. **Decide between a client-side design (A) and a store-side one (B, C or E)**, from the measurements and
   from what A taught. D only if latency becomes the goal, since it is the rewrite.
5. **The guarantees text.** E5 ("nothing is promised about which consumer gets a key") changes, and the
   hooks need their own entries: what a gain or loss promises, and that neither grants authority.

## Out of scope here

- Moving state between consumers on a handover. The hooks tell a consumer when to load or drop. Moving
  the state is the consumer's business.
- Making dkq instances themselves stateful (the roadmap's phase 4). Stickiness here is about consumers.
