---
title: "A distributed lock on DKQ — as a client of itself, and when to graduate off that"
type: research
status: draft
updated: 2026-09-10
tags: [distributed-lock, fencing, lease, self-client, baton, substrate, architecture]
---

# A distributed lock on DKQ — as a client of itself, and when to graduate off that

DKQ's `claim` already *is* a distributed lock: it takes exclusive hold of a key, hands back a lease and a
**fence token**, blocks until the key is free, and auto-releases on holder death via the sweep. The fencing
token is the thing naive locks (Redlock, famously) get wrong; DKQ has it built in. So a lock API is not new
capability — it is a matter of *packaging*. There are two ways, and the more elegant one is the idea in the
brain-dump: **make DKQ a client of itself.**

Draft; a design study, now backed by two working sketches on the `lock-sketch` branch — the
self-client (`DistributedLock`) and the dedicated store (`RedisLockStore`) — each with a spec passing
against real Valkey. The mechanism claims below are checked against the Lua.

## The self-client design: a lock is a queue with one immortal message

Model a lock `L` as its own queue holding a single key with a single **baton** message. Then every lock
operation is an *existing* queue operation, and the guarantees fall out of per-key exclusivity:

| lock op | composed from |
|---|---|
| `acquire(L, wait)` | `enqueue(L, key=L, id="baton", ∅)` then `dequeue(L, batch=1, max_wait=wait)` |
| `refresh()` | `heartbeat(receipt)` |
| `release()` | `settle(receipt, nack, retry_after=0)` |
| the fence token | the receipt `dequeue` already returns |
| block until free | `dequeue`'s long-poll — the same readiness/wake/backstop path |
| auto-release on death | the sweep reclaiming the lapsed lease |

Walk it through against the scripts:

- **First acquire.** `enqueue` of a never-seen baton id `HSETNX`es into `payloads`, `RPUSH`es to `msgs`, and
  — the key being in none of `ready`/`claimed`/`delayed` — `ZADD`s it to `ready` and wakes. `dequeue` then
  claims the key: exclusive hold, a lease, a fence token. You hold the lock.
- **Contended acquire.** A second client's `enqueue` of the same baton id hits `HSETNX == 0` and is a
  **total no-op** (verified: the whole append hangs off the `HSETNX`, so it does not touch `ready` while the
  key is held). Its `dequeue` finds the key claimed, not in `ready`, and blocks on readiness — exactly the
  queue's blocking acquire, for free.
- **Release.** `settle` with a nack and `retry_after=0`: the id is removed from `owned`, the message is
  *left in place* (nack does not `LREM`), no `delayed` entry is set (that branch is `retry_after > 0`), the
  claim ends, the fence advances, and — the key still having its baton in `msgs` and not being delayed — it
  is `ZADD`ed back to `ready` and wakes a waiter. That is release, precisely.
- **Crash.** The holder stops heart-beating; the sweep finds the lapsed lease, advances the fence
  (invalidating the dead holder's token), and returns the key to `ready`. Auto-release, precisely.

The baton is **immortal by never acking it** — release is always nack-zero, so the message stays in
`payloads`/`msgs` forever and its fixed id makes every re-`enqueue` a dedup no-op. Two clients racing to
"create" the lock both enqueue `id="baton"`, dedup collapses them, and there is exactly one baton — so
never two holders. This is the whole correctness argument, and it rests on two existing properties (HSETNX
dedup, nack-leaves-in-place) rather than any new code.

### The property that makes this worth doing: potentially zero server changes

Because acquire/release/refresh are compositions of the four existing RPCs, **the lock can be a pure client
library** — `dkq-lock` — with the DKQ server unchanged and unaware locks exist. `acquire` = one idempotent
enqueue plus a blocking dequeue; `release` = one settle. Correct fencing, crash-recovery, and blocking all
inherited. You could ship it this afternoon and validate whether anyone wants locks before writing a line of
server code.

## The one constraint it forces, and the one cost it carries

**Constraint — one queue per lock.** DKQ's `dequeue` claims the *oldest claimable key* (`ZPOPMIN`), not a
*named* one. So "acquire lock `user-42`" cannot be "dequeue key `user-42` from a shared locks queue" — you
would get whichever key happened to be free. The only way "dequeue from `L`" unambiguously means "acquire
`L`" is if `L` is its own queue with exactly one key. That is fine (queue names are just string prefixes),
but it is the reason a shared `__locks__` queue does not work and each lock is its own namespace.

**Cost — sweep fan-out.** The watchdog sweeps *every queue it has served*, every `sweep_interval`
(`queues.get.flatMap(foreachDiscard(sweep))`, verified). One-queue-per-lock means N locks = N queues swept
each pass — mostly finding nothing, since a lock is only reclaimable while held by a dead holder. At homelab
scale (dozens to hundreds of locks) this is cheap noise. At thousands it is O(locks) wasted scripts per
interval, and it is the axis on which the self-client model scales worse than a purpose-built one.

Two smaller wrinkles, both cosmetic: acquire is two RPCs (the enqueue is idempotent and could be skipped
after the first, but simplest is to always send it), and the baton's `attempts` counter climbs by one per
acquire over the lock's whole life — harmless, though a poison-message detector would eventually notice a
baton "delivered" a million times.

## The sibling alternative, for when fan-out bites

If lock volume grows enough that sweeping N lock-queues hurts, the graduation is a purpose-built
`LockStore` — a sibling to `QueueStore`, sharing the substrate machinery (connection split, `LuaScript`,
`Readiness`, wake, fence, the blocking-in-the-caller's-fiber discipline) but with its own, *simpler*
operations. A lock is a queue **minus messages, ordering, and backoff**, so the new Lua is smaller than what
exists:

- `acquire(name)` — claim a *named* key (no `ZPOPMIN`): `if not held then set lease, INCR fence, return
  token else nil`. Names the specific resource natively — the constraint the self-client model works around.
- `release(name, token)` / `refresh(name, token)` — fence-checked, as today.
- **no sweep at all** — the sketch found this. Because acquire is *named*, `lock_acquire.lua` checks the
  one lock's lease inline and reclaims it if expired, so a dead holder is released by the next contender,
  not a background pass. A lock holds no work, so a lock nobody is waiting for needs no reclaiming — the
  queue sweeps only because a dead consumer's *messages* must resurface even when its key is idle. Two
  structures (`held` zset, `fence` hash), three small scripts, and the watchdog dependency gone.
  (Prompt crash-reclaim for a *blocking* waiter still wants the backstop; `tryAcquire` reclaims inline
  regardless.)

It costs real server code — a port, three scripts, a release-triggered wake flow, a gRPC surface — to buy
back the sweep scaling and native named-acquire.

## Which, and when

| | self-client (the brain-dump) | sibling `LockStore` |
|---|---|---|
| server changes | **none** (a client library) | port + 3 scripts + wake + gRPC |
| names a specific lock | via one-queue-per-lock | natively |
| sweep cost | O(locks ever served) | O(locks held now) |
| fencing / blocking / recovery | inherited | inherited (shared machinery) |
| time to first working lock | an afternoon | a feature |

**Start with the self-client.** It is correct, it is nearly free, and it validates demand before any server
work — exactly this codebase's instinct (start minimal, measure, graduate). **Graduate to the sibling store
when** two things are both true: locks are numerous enough that sweep fan-out is a measured cost, *and*
named-acquire's two-RPC / one-queue-per-lock shape is friction rather than a curiosity. Until then the
sibling store is speculative work against a demand that may never materialise.

## Substrate note

The self-client pattern is substrate-agnostic by construction — it rides the `QueueStore` port, so it works
on Redis today and on a future Postgres adapter unchanged (`substrate-candidates.md`). The sibling store, on
Postgres, has a native form worth remembering: a **session-level advisory lock** is exactly an
ephemeral-on-death lease (`zookeeper-ideas.md`, `postgres-substrate.md`) — the lock releases when the
holder's connection drops, no sweep at all. That is the one place the purpose-built path is not just faster
to sweep but structurally simpler than either DKQ model — and a reason the sibling store, if built, might be
substrate-specific rather than a third port.

## Non-goals (either design)

Reentrancy (same holder re-acquiring — needs holder identity and a count), fairness among waiters (readiness
wakes one *arbitrary* waiter, not FIFO — fine for mutual exclusion; FIFO is the ZooKeeper
watch-your-predecessor recipe, a later thing), and lock hierarchies. Say no to all for a first cut.

## Open questions

1. Does the two-RPC acquire matter, or is one idempotent enqueue per acquire lost in the noise of a blocking
   wait that is usually the dominant cost anyway?
2. Could the watchdog learn to sweep only queues with *live claims* rather than all served queues? That
   single change would erase the self-client model's one real cost and make the sibling store almost
   pointless — worth scoping before building anything lock-specific.
3. Is a lock's baton `attempts` growth ever a problem, or purely cosmetic? Only matters if poison-detection
   is ever wired to act on the count.
