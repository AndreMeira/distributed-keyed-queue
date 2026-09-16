---
title: "How the lock works — structures, state transitions, and the code that drives them"
type: architecture
status: current
updated: 2026-09-16
tags: [lock, redis, lua, tickets, fairness, fencing, readiness, wake, architecture]
---

# How the lock works

What the lock *promises* is [`lock-guarantees.md`](lock-guarantees.md). What it *keeps in Redis* is the
lock section of [`redis-data-structures.md`](redis-data-structures.md). This page is the mechanism between
them: the states a lock moves through, the script that performs each move, and the code that waits.

A lock is a **per-name exclusive lease**: one holder at a time, for a bounded time, with a fencing token
that rises on every grant. Waiting is **first-come-first-served**, and the order lives in Redis rather than
in any instance.

## The five structures, and what each decides

Everything for one partition, under `{p:<partition>}:v3:l` — one set of structures shared by every lock in
it, with the lock's name as a member or field. `{L}` below is that prefix.

| structure | type | decides |
|---|---|---|
| `{L}:held` | zset, `name -> lease deadline` | **whether it is held.** A score in the future means held; anything else is free |
| `{L}:tokens` | hash, `name -> fence token` | **who holds it.** Release and refresh match against this, so a displaced holder is refused |
| `{L}:fence` | string counter | **the fence.** `INCR` on every grant, and again for each ticket id |
| `{L}:waiters:<name>` | list of `id:deadline` | **the order.** Arrival order; head is next |
| `{L}:waiting` | zset, `name -> latest ticket deadline` | **which waiter lists exist**, so the trim can find abandoned ones without scanning |

The wake stream is not the lock's own: it is the partition's `{p:<partition>}:v3:wake`, shared with the
queue, with each entry saying which kind it is ([`redis-cluster.md`](redis-cluster.md)).

## The three states

```
                    acquire / tryAcquire, nobody queued
        ┌──────────────────────────────────────────────────┐
        │                                                  ▼
    ╭────────╮                                        ╭─────────╮
    │  FREE  │◀──── release (token matches) ──────────│  HELD   │
    │        │◀──── trim (expired past grace) ────────│         │
    ╰────────╯                                        ╰─────────╯
        ▲                                                  ▲
        │  last ticket expires or is withdrawn             │ grant, to the head ticket only
        │                                                  │
    ╭─────────╮                                            │
    │ QUEUED  │────────────────────────────────────────────┘
    ╰─────────╯   acquire when held or already queued → tail ticket
```

**FREE** is the absence of state: no entry in `held`, no waiters list. An idle lock costs nothing, and a
lock nobody has ever taken is indistinguishable from one released long ago.

**HELD** is an entry in `held` with a future deadline plus an entry in `tokens`. The pair is written by one
script, always together.

**QUEUED** is a non-empty `waiters:<name>`. A lock can be queued *and* free for an instant — held by nobody,
with tickets waiting — which is why `tryAcquire` refuses then: taking it would be barging.

## Who performs each transition

Seven scripts, each atomic, each touching only its partition's keys:

| script | moves | how |
|---|---|---|
| `try.lua` | FREE → HELD | grants only when free **and** unqueued; reclaims an expired lease inline |
| `acquire.lua` | FREE → HELD, or → QUEUED | grants if free and unqueued, else takes a tail ticket and says when to look again |
| `grant.lua` | QUEUED → HELD | grants only to the head ticket, and only when the lease has lapsed |
| `refresh.lua` | HELD → HELD | pushes the deadline forward if the token still matches |
| `release.lua` | HELD → FREE | deletes both entries and appends the wake |
| `abandon.lua` | QUEUED → (one ticket fewer) | withdraws a named ticket |
| `trim.lua` | HELD → FREE | removes holds expired past a grace, and deletes waiter lists whose every ticket is long dead |

Three properties fall out of *where* the work is, rather than out of care taken by callers:

- **Expired tickets are pruned by whoever passes.** `try`, `acquire` and `grant` all pop dead tickets off
  the head before reading anything. A waiter that dies delays nobody past its own patience, and no
  background pass is needed for the common case.
- **A dead holder is reclaimed inline.** An expired lease is simply not a live one, so the next grant
  overwrites it. There is no separate recovery step on the acquire path.
- **The wake cannot precede the state it announces.** `release` and `trim` append to the wake stream *in the
  same script* that frees the lock, so no waiter is ever sent to look at a lock still held, and no crash can
  land between freeing and telling.

## Waiting, and why it is not polling

A blocking `acquire` that cannot be granted comes back with a ticket and a **delay** — not a deadline — after
which the answer could change: the lease's remaining time when the lock is held, the head ticket's remaining
patience when queued behind it. A delay rather than an instant, so the caller never compares its clock to
the store's.

The waiter then parks on a mailbox for at most that long, and wakes on whichever comes first:

1. **the wake stream** — a release or trim on *any instance* appends an entry; the shared
   [wake path](#the-code) delivers it to this instance's `LockReadiness`, which wakes **every**
   local waiter on that name;
2. **the recheck delay** — nothing was announced, but the event the refusal named has now passed
   (a lease expired with no release, because the holder died);
3. **its own patience** — it gives up, withdraws its ticket, and returns nothing.

Every woken waiter asks, and `grant.lua` refuses all but the head — so the woken crowd is a **check, not a
race**. That is the cost of fairness, and it is bounded: one grant attempt per local waiter per event.

Two details make this sound rather than merely plausible:

- **The mailbox is subscribed before entering.** A release landing between "ask" and "park" would otherwise
  be missed, and the waiter would sleep until its recheck for no reason.
- **The ticket is withdrawn on every exit that is not a grant** — patience spent, caller interrupted, effect
  failed. A ticket left behind holds up the queue until its deadline.

## The code

| piece | role |
|---|---|
| `RedisLockStore` | the adapter: one method per script, plus the waiting loop (`queued` → `awaitTurn` → `turn`) |
| `LockReadiness` | per-name mailboxes; a wake reaches **every** subscriber and is **dropped** if nobody waits |
| `QueueReadiness` | the queue's counterpart, for contrast: one token to **one** consumer, and **kept** if nobody waits |
| `WakeConsumer` | one blocking `XREAD` per partition, fanned into one batched intake |
| `ReadinessProcessor` | routes each entry to one readiness by the kind it carries |
| `LockCleanup` | the periodic `trim` — the only background pass the lock has |
| `KeyLayout` / `LockKeys` | which partition a name falls in, and the keys that follow from it |

**Why the lock has its own readiness.** The two answer opposite questions, and each depends on its own
answer. A queue's wake says work exists; any consumer may take it, and the wake must survive nobody being
parked — so it is one token, kept. A lock's wake says a *specific* name came free; only the head ticket may
proceed, and the store alone knows which waiter that is — so it reaches everyone, and may be dropped,
because a waiter subscribes before it enters. Handing the lock's wakes to the queue's readiness gives the
single token to a waiter that may not be next, and the head sleeps: that was a real stall, caught by the
contention spec.

**The waiting loop, in three methods.** `awaitTurn` computes the window (the shorter of the remaining
patience and the time to the next recheck) and parks; `turn` runs `grant.lua` and interprets the three
answers — granted, wait-this-long, ticket-gone; a gone ticket means the whole enter is redone, since the
ticket expired while its owner was still willing to wait.

## Where to look next

- [`lock-guarantees.md`](lock-guarantees.md) — the invariants, and what is deliberately *not* promised
- [`../learning-material/lock-state-walkthrough.md`](../learning-material/lock-state-walkthrough.md) — the
  same mechanism as concrete key-by-key state, call by call
- [`redis-data-structures.md`](redis-data-structures.md) — every structure, queue and lock
- [`../research/distributed-lock.md`](../research/distributed-lock.md) — why it is a ticket lock, and what
  was tried first
