---
title: "Should waiting invert? — the store blocks today, and the readiness is passed to it"
type: research
status: draft
updated: 2026-09-14
tags: [readiness, waiting, ports, domain, lock, queue, hexagonal, what-if]
---

# Should waiting invert?

Written 2026-09-14 while the `redis` package was being straightened, and parked deliberately: the question
is real, the answer looks clear, and none of it needs doing before the current refactor lands. Recorded so
it does not have to be re-derived.

## The thing that is wrong

`QueueStore` and `LockStore` both promise "this call waits". The Redis adapters keep that promise by holding
a `QueueReadiness` / `LockReadiness` and running the waiting loop themselves. So an **outbound** port takes a
coordination collaborator in order to do work that is not the substrate's — and the wiring points the wrong
way round.

Evidence gathered rather than assumed:

- **Each readiness has exactly two consumers**: one store's waiting code, and the wake processor. Nothing
  else in the codebase touches them.
- **Eleven of `RedisLockStore`'s sixteen methods never touch a script** — `queued`, `awaitTurn`,
  `nextEventIn`, `untilRecheck`, `atLeastFloor`, `remainingTime`, `hold`. Hold a ticket, park until the next
  known event or until the patience runs out, withdraw on any exit that is not a grant. That is the lock's
  contract, living in an adapter.
- **`patience` never reaches a script.** `Demand` and `Acquisition` both carry it; only the loop reads it.
- **There is no in-memory adapter** (`infrastructure/` is `codecs`, `configuration`, `redis`, `tracing`),
  and there cannot easily be one: a second adapter would have to reimplement the waiting.

## What the ports would become

**`QueueStore` barely changes** — its waiting is a retry over one operation:

```
claim(demand: Demand): Option[Grant]     ->   attempt(queue: QueueName, batch: Int): Option[Grant]
```

**`LockStore` grows**, because its waiting is a protocol rather than a retry:

```
acquire(acquisition): Option[Hold]       ->   enter(acquisition): Entered          -- granted | queued(ticket, recheck)
                                              grant(name, ticket, ttl): Asked      -- granted | wait(recheck) | gone
                                              abandon(name, ticket): Unit
tryAcquire / release / refresh / trim         unchanged — none of them wait
```

`Entered` and `Asked` become domain types. They sit in `infrastructure/redis/script/lock/` today, but nothing
in them is Redis: granted, queued behind a ticket, not yet — try again in `d`, your ticket is gone. Any fair
lock must answer exactly those. **`recheck` is a duration, not an instant**, so a caller never compares the
store's clock to its own; promoted to a port that becomes a published guarantee rather than an accident of
the Lua.

## The inversion, and where it lands

The tempting move is to reverse the dependency: the readiness takes the store and blocks itself. It works,
and it renames itself in the process — once a thing holds a store and answers with a `Grant`, it is the
waiter, and readiness is its internal state.

The lock decides the final shape. Its waiting is a protocol, so a `LockReadiness` holding a `LockStore`
**is** the acquire use case. Which gives the honest version:

> The waiting is a stateful domain service, and its coordination primitive is private to it.

`QueueWaiting(store)` owning tokens; `LockWaiting(store)` owning mailboxes and tickets. Use cases delegate,
ports go non-blocking, and nothing in the domain takes a readiness as a parameter because nobody outside
needs one. Under that arrangement the wake path is an **inbound adapter** — `WakeConsumer` its transport,
`ReadinessProcessor` its handler, `ready`/`readyAll` the domain operations it drives — structurally the same
as `LockService` driving use cases over gRPC. That is why the current wiring felt wrong: an inbound adapter
was reaching into a collaborator an outbound adapter happened to own.

## Recommendation, and what it costs

Do it as domain services, not by giving the existing `*Readiness` classes a store. **Queue first** — a retry
loop that proves the port change cheaply; **lock second**, where the protocol either holds or does not.

**Protect one thing:** keep the token buffer as its own small type with its own spec. `QueueReadinessSpec`'s
500 rounds racing a token against a timeout and an interrupt are the sharpest tests here, and they work only
because that primitive is testable without a store. An extra type is cheap insurance.

**Two costs to accept:** the wake processor's dependency widens from "a sink with `ready`" to a service that
can also claim — resist re-adding a `Waker`-style trait to narrow it, since that trait was just removed for
good reasons. And `DistributedLock`, which builds a lock over the queue with
`claim(Demand(name, patience, 1))`, is a caller that is not a use case; it needs the new service or its own
loop.

**The counterweight:** the current shape works and is documented. The payoff is coherence, plus making a
second adapter — in-memory, or Postgres ([`postgres-substrate.md`](postgres-substrate.md)) — actually
possible, since today the waiting would have to be written again per substrate. If no second adapter is
coming, this is a truth-in-layout change rather than a capability one.
