---
title: "ZooKeeper ideas worth stealing for DKQ — mostly a mirror, and one latent option"
type: research
status: draft
updated: 2026-09-10
tags: [zookeeper, lease, session, ephemeral-nodes, wake, herd-effect, architecture, convergence]
---

# ZooKeeper ideas worth stealing for DKQ — mostly a mirror, and one latent option

`substrate-candidates.md` rejects ZooKeeper *as a substrate* (small values, low write throughput) but flags
one property — the ephemeral node as a self-cleaning lease — as worth stealing regardless of substrate.
Working that through carefully, the honest result is more useful than a shopping list: DKQ has
independently arrived at most of ZooKeeper's good ideas, the one it hasn't optimises a case DKQ already
sidesteps, and one ZK pattern is an *anti*-pattern DKQ is already past. This note is that analysis.

Draft; a design study, nothing to build.

## The headline: the ephemeral node, and why DKQ already has its behaviour

ZooKeeper's most-praised primitive is the **ephemeral node tied to a session**. A client keeps a session
alive by pinging the ensemble; its ephemeral znodes exist only while the session does. Crash, and after the
session timeout the ensemble deletes every one of the client's ephemerals automatically — no cleanup
process, no per-node bookkeeping. The classic lock recipe leans on it: your lock *is* an ephemeral node, so
a dead lock-holder releases its lock by dying.

The obvious steal: make a DKQ claim an ephemeral thing, and a dead consumer releases its claim for free —
deleting the lease, the watchdog, and the sweep's reclaim pass. Except three facts, checked against the
code, drain most of the value out:

**1. ZK's session is a heartbeat-with-timeout, exactly like DKQ's lease.** The ensemble does not detect
death; it detects *silence* — no ping within the session timeout. DKQ's `claimed_until` + `renew` is the
same mechanism: renew is the ping, the deadline is the timeout, the sweep is the reaper. The detection
latency is identical (one timeout), so ZK is not *faster* at noticing a dead consumer — it is the same
idea with the reaping moved into the server.

**2. DKQ already heartbeats per *consumer*, not per *claim*.** The tempting refinement — "one ping keeps
all my claims alive, like one session covers all my ephemerals" — is already true: `HeartbeatUseCase`
takes *every* receipt a consumer holds and renews them in one call
(`store.renew(renewal.held)`). A consumer sends one `Heartbeat` RPC and all its claims live or die
together. That *is* session semantics; DKQ spells it as a list of deadlines pushed forward together rather
than as one session record, but the consumer-facing behaviour is identical.

**3. DKQ deliberately removed the per-consumer record the session idea would re-add.** `RenewScript` says it
outright: *"A consumer is not a worker with a registration of its own: its claims are found by fence
token."* The store has *"no per-connection identity and no recovery for one."* This was a chosen
simplification — an earlier design (the homelab's `registration-service`) had worker registration, and DKQ
dropped it. A ZK-style session is precisely a worker registration under another name; stealing it walks back
a decision made on purpose.

So what would actually change? Only the storage-layer cost of the reap. Today `RedisQueueStore.renew`
groups a consumer's claims by queue and runs one script per queue — a consumer spanning three queues pays
three round trips (still one RPC). A session record would collapse those to one heartbeat against one
session key, and the sweep would reclaim by "session key expired" instead of "deadline passed". That is the
whole prize: **fewer storage round trips for a consumer that spans many queues.**

And DKQ already discourages exactly that shape — the README's known limitation is *"one queue per
`Dequeue`; a consumer spanning queues needs a connection each."* The common consumer works one queue, where
renew is already a single round trip. So the session idea optimises the multi-queue case the usage model
already steers away from.

**Verdict: understood, not stolen.** It is elegant, DKQ's behaviour already matches it, and its one
structural win (auto-reap) buys a round trip only in a case DKQ discourages — while re-adding a concept DKQ
deliberately deleted, and *not* eliminating the sweep, which is still needed for the backoff-release pass
regardless. Steal it only if a real deployment makes many-queue consumers common and per-queue renew round
trips measurably hurt; the trigger is concrete, and until it fires this is the wrong trade.

## Convergent validation: three ZK recipes DKQ already implements

More valuable than a steal: three places DKQ independently landed on ZooKeeper's hard-won answers, which is
reason to trust them.

**Wake exactly one, not the herd.** ZooKeeper's lock recipe is famous for what the *naive* version gets
wrong: every waiter watching the lock node means a release wakes them all, they stampede, one wins, the rest
re-sleep — the herd effect. ZK's fix is each waiter watches only its predecessor, so a release wakes exactly
one. DKQ's `QueueReadiness` reaches the same "wake one" by a different route — a single-token buffer, one taker
per token — and its whole reason for existing is stated as avoiding the broadcast that "wakes every consumer
so one wins and the rest waste a round trip." Same problem, same principle, simpler mechanism: DKQ needs no
predecessor chain because, unlike lock waiters, its consumers are interchangeable — any one can take any
key, so waking an *arbitrary* one is enough and waking them *in order* is not required.

**The wake cannot precede the state.** ZooKeeper guarantees a client sees a watch fire only after the state
change that triggered it is visible. DKQ gets the same ordering by construction: `enqueue.lua` does the
`XADD` to the wake stream inside the *same script* that adds the key to `ready`, so a woken consumer can
never arrive before the work it was told about. Convergent, and for the same reason it matters — a wake that
outran its state would send consumers to look at nothing.

**A client learns it is stale.** When a ZK session expires, the client is told, so it stops acting as
though it still holds its ephemerals. DKQ's heartbeat does the same: a receipt that can no longer be renewed
comes back in `HeartbeatResponse.stale`, and the contract requires the consumer to stop working it. The
guarantee DKQ cannot enforce — a consumer must *heed* the stale report — is exactly the one ZK cannot
enforce either.

## The anti-pattern DKQ is already past: one-shot watches

Not everything in ZooKeeper is worth having. Its watches are **one-shot and edge-triggered**: a watch fires
once, for one change, and must be re-registered — and in the gap between firing and re-arming, changes are
missed. This is the precise fragility the DKQ wake conversation settled against (`minimal-structures.md`,
and the `QueueReadiness` backstop discussion): correctness must rest on re-readable *state* (level-triggered),
with the edge wake as a best-effort accelerator. DKQ's claim reads real state every time and a lost wake
costs only latency; a system built on ZK watches alone must get the re-arm race right on every recipe. DKQ
is more robust here than the thing we came to learn from — worth stating plainly, because it is easy to
assume the older, famous system is ahead on every axis.

## The one substrate with a real session primitive: Postgres advisory locks

If the session-as-lease idea ever *is* worth taking, one candidate substrate has a native form of it, and it
is not ZooKeeper. A PostgreSQL **session-level advisory lock** (`pg_advisory_lock`) is held for the life of
a connection and released automatically when the connection drops — a genuine ephemeral-on-session-death,
exactly ZK's property, in a store DKQ would actually run (`postgres-substrate.md`). A claim could take an
advisory lock keyed by the claim; a dropped consumer connection releases it; the sweep reclaims claims whose
advisory lock is free. It trades the deadline sweep for connection-liveness — with the same caveats as ZK
(a wedged-but-connected consumer still holds it, detection is still timeout-shaped via TCP keepalive), and
it pins a connection per consumer, which fights the pooler the same way `LISTEN` does. A latent option for a
latent substrate; noted so the connection is not lost, not recommended.

## Net

- **Steal now: nothing.** The headline idea is behaviour DKQ already has.
- **Trust more: three convergences** — herd-free wake, wake-ordered-after-state, learn-you-are-stale — are
  DKQ independently matching a battle-tested system.
- **Latent option with a trigger:** session-as-lease, worth revisiting only if many-queue consumers become
  common *and* per-queue renew round trips measurably hurt — and if so, Postgres advisory locks are the
  natural home, not a ZK dependency.
- **Already ahead:** DKQ's level-triggered wake is more robust than ZK's one-shot watches; do not regress
  toward them.

## Open questions

1. Is there a workload where a single consumer legitimately spans many queues, or is "one queue per
   consumer" a permanent property of the model? The session idea's entire value hinges on the answer.
2. Would folding the reap into an auto-expiring session key actually simplify the sweep, or just split it
   (session-expiry reclaim + backoff release) into two mechanisms where there is now one? Suspect the
   latter, which is another argument against.
