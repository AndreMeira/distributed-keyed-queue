---
title: "Key stickiness on top of the lock API — shard queues, owned by locks, shared fairly"
type: research
status: draft
updated: 2026-11-10
tags: [stickiness, affinity, sharding, lock, fairness, membership, rebalancing, hooks, client, exploration, not-a-decision]
---

# Key stickiness on top of the lock API

> **Exploration, not a decision.** This works out strategy A of
> [`key-stickiness.md`](key-stickiness.md) in detail, under one constraint: **the server's API changes as
> little as possible**. The base design needs no server change at all. One of the two fairness options
> adds a single field to one response. Everything else lives in the client.

## The shape

- **Shard the queue.** A queue `orders` becomes N queues, `orders.0 … orders.<N-1>`. A sharded producer
  sends key `k` to `orders.<hash(k) % N>`. Every message of one key therefore lands in one shard, and keeps
  its per-key order there.
- **One lock per shard.** Consumer C works shard `s` only while it holds the lock `orders.shard.<s>`
  through DKQ's own lock API: `Acquire`/`TryAcquire`, `Refresh`, `Release`.
- **Stickiness follows.** While C holds shard `s`, every key that hashes to `s` goes to C, and so keeps
  whatever state C has warm for it.
- **The hooks are the lock's events.** Acquiring a shard's lock is **gaining** it. Releasing it, or a
  `Refresh` answering `renewed = false`, is **losing** it. These are per shard, so they're bounded, unlike
  per-key hooks.

### Why "mostly right" is enough

The queue already enforces one consumer per **key** on every claim (E1). If two consumers briefly both
believe they own shard `s`, during a handover or while their views of membership differ, they can't work
the same key at the same time. They only compete for its claims. A mistaken ownership costs locality,
never correctness. So the lock does not have to be exact, and nothing here needs consensus.

## The problem: sharing the shards fairly

With N shards and M live consumers, each consumer should hold ⌊N/M⌋ or ⌈N/M⌉. The lock gives the
exclusivity and the handover, but two pieces are missing:

1. **The fair share.** Nothing in the lock API reveals M. It doesn't list holders, and doesn't count them.
2. **A reason to give a shard up.** The lock hands a released lock to its longest waiter, in ticket
   order, but a holder is never told that someone is waiting. Today `Refresh` answers only
   `renewed`, `lease_expires_at` and `lease_ttl`.

Two ways to supply them follow.

## Option A: contention-driven yielding

**The protocol:**
1. **Claim up to the share.** A consumer takes free shards with `TryAcquire`, in its preferred order
   (rendezvous order over shard ids, so preferences are stable), until it holds ⌈N/M⌉.
2. **Queue for one more when below the share.** It parks a blocking `Acquire` on one shard held by
   someone else, preferring the shards it would win under rendezvous. That puts it in the lock's FIFO queue.
3. **Yield when above the share and contended.** A holder whose `Refresh` reports waiters, and which holds
   more than ⌈N/M⌉, releases that shard at its **next safe point**: after the claim in flight is settled,
   never mid-handler. The lock's queue then hands the shard to the waiter.
4. **Hysteresis.** Release only above ⌈N/M⌉, and only one shard per refresh. Without that, two consumers
   can trade a shard back and forth.

**The API change it needs:** `RefreshResponse` gains one field, `waiters` (the number of live tickets) or
`contended` (whether there is one).
- `refresh.lua` already runs on the partition that holds `waiters:<name>`.
- Its live length is a prune of expired tickets from the head (as the grant already does), then `LLEN`.
- It's additive. An old client ignores the field, so no schema bump is needed.

It still needs M, from one of the membership sources below. Option A's contention signal is what lets a
consumer above its share *notice* that someone needs a shard, without anyone computing a global assignment.

## Option B: assignment computed from membership

**The protocol:**
1. **Every consumer knows the member list** (sources below), and computes the shards it should own, for
   example by rendezvous hashing of `(shard, member)`, keeping the highest score. Rendezvous hashing moves
   only about 1/M of the shards when one member joins or leaves. Plain `s % M` over the sorted members is
   exactly even, but moves more.
2. **Acquire what you should own.** For each shard it should own and doesn't hold, the consumer parks a
   blocking `Acquire`.
3. **Release what you no longer own.** For each shard it holds but shouldn't, it releases at its next safe
   point.
4. **The lock makes the handover clean.** The new owner is already queued, so the shard passes to it the
   moment the old owner lets go.

Fairness holds by construction, nobody negotiates, and it needs **no server change at all**.

### Where membership comes from

| source | needs | strengths | costs |
|---|---|---|---|
| **StatefulSet**: ordinal + replica count (config) | a StatefulSet | exactly even; identities survive restarts (static membership) | a reassignment on every scale |
| **Headless Service DNS**: the ready pods' IPs | a headless Service; no RBAC, no client library | follows scaling, autoscaler included; readiness-gated | DNS caching: views can differ for a few seconds (harmless, see above); identities change on every restart |
| **Kubernetes API**: `EndpointSlice`s, watched | a Kubernetes client, and RBAC to read them | fresher than DNS, pushed rather than polled | ties the consumer to Kubernetes |
| **Slot leases on DKQ's lock**: `orders.slot.0 … orders.slot.<M-1>`, M from config | M in config next to `replicas:` | exactly even; a slot number is a stable ordinal; nothing outside DKQ | M is duplicated in the manifest; no autoscaler |

**Slot leases in more detail.**
- A pod takes the first free slot with `TryAcquire` and keeps it with `Refresh`. Its slot number is its
  ordinal, and it owns the shards `s % M == slot`.
- **A dead pod's slot** lapses on its lease, and a new pod takes it.
- **A pod with no free slot**, during a rolling update with surge, parks on the slots and owns nothing
  until one frees.

**Without stable identities, a restart moves shards.** Every restart of a pod is a leave plus a join, which
rendezvous hashing keeps to about 1/M of the shards. A rolling update moves each pod's share once, one
pod at a time. Only a StatefulSet or slot leases avoid that.

## The handover, step by step

The same steps serve both options, when consumer C gives shard `s` to D:

1. **D is queued** on `Acquire(orders.shard.<s>)`. Its patience is at most `max-wait` (30 s by default),
   so it re-queues in a loop until it gets the lock.
2. **C decides to let go**, because its share changed (B) or it's above its share and contended (A).
3. **C finishes what it has in flight.** It stops dequeuing from `orders.<s>`, settles the claims it holds,
   and runs its `onRevoked(s)` hook (flush or drop the warm state).
4. **C releases the lock.** The lock grants it to D, its longest waiter.
5. **D runs `onAssigned(s)`** (load or warm state) and starts dequeuing from `orders.<s>`.

Between steps 3 and 5, `orders.<s>`'s messages wait. That is the price of the handover, a few hundred
milliseconds plus the hooks' own time.

**If C dies instead**, its lock lapses at its TTL, and D gets it then. The keys of shard `s` wait up to one
lock TTL. The lock's TTL is therefore a trade-off: short for fast recovery, long enough that a pause
(GC, a slow handler) doesn't lose the shard.

**`onRevoked` can also come without a release.** A `Refresh` answering `renewed = false` means the lock was
lost, for example after a long pause. C must stop working the shard at once. Any claim it still holds is
still valid on its own lease, and the queue's fence keeps a stale holder from settling anything after it.

## Things to keep in mind

- **Shards ≫ consumers.** With 8 shards and 3 consumers, the ±1 rounding is a 3-against-2 split. With 64
  shards it is 22 against 21. A good default is 8 to 16 shards per expected consumer.
- **Changing N is a resharding.** It moves keys between shard queues, and a key's old messages in
  `orders.3` and new ones in `orders.7` would break its order. Either drain first, or plan a two-phase move
  (producers switch, consumers drain the old shard before taking the new one). Pick N generously once.
- **Idle shards cost little.** A shard queue nobody sends to is just keys that aren't there. Its lock is one
  entry in `held` and a refresh per beat.
- **Two known lock issues matter here** (from `../sessions/2026-11-02-old-review-threads.md`):
  - **`abandon.lua` wakes nobody.** A waiter that gives up lets the next one sleep until its old deadline,
    which slows a handover when D's patience expires and it re-queues.
  - **The grant is O(n²) under contention.** With a handful of waiters per shard that's negligible, but
    it would matter if many consumers queued on one shard.
- **The beat must run while idle.** Holding shard locks needs refreshes even when no claim is held, unlike
  the managed client's queue `Heartbeat`, which beats only while claims are held. The lock refresh loop is
  its own beat.

## What would be built, and where

- **In the client** (`homelab.keyedqueue.client`), all of it:
  - a sharded producer (key → shard queue);
  - a shard owner, which runs the protocol, the handover and the hooks;
  - a sharded consumer that dequeues only from the shards it owns;
  - a membership source behind a small trait (DNS, slots, static config).
- **In the server:** nothing for option B. For option A, the `waiters` (or `contended`) field on
  `RefreshResponse`, and the small change in `refresh.lua` that reads it (the script would also take the
  waiters key it does not take today).

## How to measure it

- **Stickiness rate:** the share of claims whose key was last worked by the same consumer. Compare it with
  today's accidental stickiness on the same e2e shapes (round 1 of `key-stickiness.md`).
- **Handover latency:** from `onRevoked` on the old owner to the first dequeue on the new one.
- **Balance:** the spread of shard counts across consumers, at rest and through a rolling restart.

## Open questions

- **Option A or B**, or B first with A's field added later.
- **The membership source by default:** DNS for elastic deployments, slot leases for fixed ones?
- **N per queue:** a client setting, or recorded somewhere the producer and consumers both read.
- **The hook surface:** per shard only, or a per-key "first seen since gain" convenience on top.
- **How the hooks meet the managed consumer:** whether `onRevoked` blocks the release until it returns,
  and with what timeout.
