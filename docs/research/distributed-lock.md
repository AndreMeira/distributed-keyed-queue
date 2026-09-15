---
title: "A distributed lock on DKQ — as a client of itself, and when to graduate off that"
type: research
status: draft
updated: 2026-09-15
tags: [distributed-lock, fencing, lease, self-client, baton, substrate, architecture, fairness, tickets]
---

# A distributed lock on DKQ — as a client of itself, and when to graduate off that

DKQ's `claim` already *is* a distributed lock: it takes exclusive hold of a key, hands back a lease and a
**fence token**, blocks until the key is free, and auto-releases on holder death via the sweep. The fencing
token is the thing naive locks (Redlock, famously) get wrong; DKQ has it built in. So a lock API is not new
capability — it is a matter of *packaging*. There are two ways, and the more elegant one is the idea in the
brain-dump: **make DKQ a client of itself.**

Draft; a design study. Both designs were built as working sketches — the self-client (`DistributedLock`)
and the dedicated store (`RedisLockStore`) — each with a spec passing against real Valkey. The dedicated
store is what ships; the self-client sketch was removed on 2026-09-15 (addendum at the end). The mechanism
claims below are checked against the Lua.

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
`QueueReadiness`, wake, fence, the blocking-in-the-caller's-fiber discipline) but with its own, *simpler*
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

Reentrancy (same holder re-acquiring — needs holder identity and a count) and lock hierarchies. Fairness
was on this list for the first cut; it graduated to a design of its own — see the ticket addendum below.

## Open questions

1. Does the two-RPC acquire matter, or is one idempotent enqueue per acquire lost in the noise of a blocking
   wait that is usually the dominant cost anyway?
2. Could the watchdog learn to sweep only queues with *live claims* rather than all served queues? That
   single change would erase the self-client model's one real cost and make the sibling store almost
   pointless — worth scoping before building anything lock-specific.
3. Is a lock's baton `attempts` growth ever a problem, or purely cosmetic? Only matters if poison-detection
   is ever wired to act on the count.

## Addendum (2026-09-11): the fence became one global counter

The sibling store was built, and one structure changed on the way: the per-lock `fence` hash
(`name -> HINCRBY`) became a single global `INCR` counter plus a `tokens` hash (`name -> the live holder's
token`) that is deleted on release.

Why: fences only need comparing within one lock, and a globally increasing number satisfies that a
fortiori — but a *per-lock* counter can never be deleted, because monotonicity must outlive the hold (the
next grant for a name must exceed every fence ever issued for it, or a stale holder's number could win
downstream). Per-name counters therefore grow with every lock name ever used. With the counter global, the
only key that outlives a hold is one integer; the per-name entry carries no history and dies with its
release.

The residue that remained — a holder that dies and whose lock is never acquired again leaves one `held`
member and one `tokens` field — is closed by a watchdog-style cleanup fiber (same day): every instance runs
a batch-limited trim every `DKQ_LOCK_TRIM_INTERVAL`, removing holds expired longer than
`DKQ_LOCK_TRIM_GRACE` and waking any waiter on each. The grace bounds the "late, not lost" refresh window,
which is now an explicit contract rather than an accident of nothing cleaning up.

## Addendum (2026-09-11): fairness — the ticket design

The dedicated store shipped unfair, and it turns out the self-client would have been too: the queue's FIFO
orders *messages within a key*, never *competing consumers* — a re-enqueued baton wakes every blocked
dequeuer and they race `claim.lua`, exactly as the lock's waiters race `tryAcquire`. Fairness was never on
offer from either design, because it needs something neither has: **an ordered record of waiters in the
store**. The queue keeps an ordered list of messages with a lease and recovery; nothing keeps an ordered
list of waiters.

The reason a waiter list looked expensive — waiters would need leases and heartbeats, rebuilding the
queue's claim machinery — dissolves on one observation: **a waiter's patience is already its deadline.**
Acquire knows, at registration, the exact instant this waiter stops mattering. A ticket carries it, and
dead waiters cost nothing beyond an inline prune.

### The design

- **Ticket on acquire.** The blocking acquire's first act is one script call that either grants immediately
  (lock free, no live tickets ahead) or appends a ticket `(id, deadline)` to a per-name waiters list —
  `{dkq:locks}:waiters:<name>`, built from the shared prefix the way `claim.lua` builds per-key structures,
  so it stays in the slot. `id` from the global counter (identity only — order is list position; it cannot
  double as the fence, see below), `deadline` = this waiter's patience end on the store's clock. The list is
  transient: created by the first waiter, deleted when the last ticket leaves.
- **Grant condition.** Lock free (or lease expired — inline reclaim as today) *and* my ticket is head among
  live tickets. The granting script prunes expired tickets from the head first, so an abandoned waiter
  delays nobody past its own patience.
- **Wake stays broadcast.** Release and trim append to the wake stream as today; every parked waiter
  re-runs the granting script; only the head can win, so the race becomes a check. Losers repark.
- **Barging dies structurally.** A newcomer's ticket goes to the tail, and `tryAcquire` refuses whenever
  live tickets exist — "free now" means free *and unclaimed by anyone who queued first*.
- **Abandon.** A waiter whose patience elapses removes its ticket on the way out (best effort; the prune is
  the backstop).

### Why the ticket id is not the fence

Tempting — both come from the same counter — but the fence must be minted **at grant**, not at
registration. A waiter registers ticket 10 while the lock is held under fence 12 (registration during a
hold is what waiting *is*); granting it fence 10 after release would hand out a fence below one already
seen downstream. Ticket order decides *who*; a fresh `INCR` at grant decides *what number*, and stays
per-lock monotonic.

### The edge that forces deadline-aware waiting

Head ticket expires while the lock sits free: no release is coming, so nothing wakes ticket two — it would
sleep out its whole patience beside an available lock. The granting script therefore returns, with every
refusal, the earliest instant the answer can change (lease expiry when held, head-ticket deadline when
queued behind the head), and the waiter parks until `min(that, own patience)` instead of parking blind.
Reactive, not polling: each re-check is at a *known event time*, the same discipline that kept the backstop
out of the wake path. This is the deadline-aware wait already wanted for the dead-holder case; the two
needs share one mechanism.

### Hygiene

A waiters list whose every ticket expired unseen (no later acquire ever visits the name) is the same
abandoned-residue shape the trim already handles for holds, and joins the same pass: an index of waiting
names with their latest ticket deadline lets the cleanup delete dead lists past the grace, batch-limited.

### What it costs, what it changes

Roughly: the acquire script grows a ticket branch, one granting script, one abandon script, one transient
structure plus its index, and the store's wait loop learns deadlines. The contract sharpens rather than
changes: grants follow registration order among waiters still within patience; `tryAcquire` refuses when a
queue exists; patience still bounds everything; mutual exclusion and the fence are untouched. This is the
ZooKeeper watch-your-predecessor recipe (`zookeeper-ideas.md`) with broadcast wake standing in for the
predecessor watch — the store keeps the order, the instances keep no state.

### Built (same day), and one thing implementation taught the design

The ticket design above is implemented — `try`/`acquire`/`grant`/`abandon` scripts, the waiters list and
`waiting` index, deadline-aware parking, trim deleting dead lists — with one discovery the sketch missed:
**`QueueReadiness` cannot carry a fair lock's wake.** Its one-token-one-consumer hand-off is the queue's whole
point (any woken consumer can claim whatever is ready), but under tickets only the head may proceed — so a
non-head waiter eats the token, is refused, and the head is never woken. The contention spec caught it as a
stall. The lock now wakes through a `LockReadiness` (every parked waiter's mailbox, subscribed *before* the
enter so no release slips into the gap), and the listener delivers to one or the other by the kind each
entry carries. The cost is as priced: one grant attempt per local waiter per event, each a check the
store answers by ticket order.


## Addendum (2026-09-15): the self-client sketch is removed

`DistributedLock` — the executable form of the design above — is deleted, along with its spec.

*Which, and when* recommended starting with the self-client and graduating later. What happened instead is
that the dedicated store was built the next day and shipped, and the sketch stayed where it was: nothing in
the service ever referenced it, and its only caller was its own spec. What it carried by the end was a
second vocabulary for the same idea — its companion held a `LockName` of its own, shadowing the domain's
inside the file — and a second answer to "how does a caller take a lock".

The design keeps its value as the record above: a lock **is** a keyed queue with one immortal message, it
needs no server code, and it rides the `QueueStore` port onto any substrate. If sweep cost or a substrate
change ever reopens the question, this is where the reasoning starts.
