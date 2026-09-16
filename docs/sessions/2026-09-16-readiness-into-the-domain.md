---
title: "Checkpoint: readiness becomes a domain concern, and the store stops waiting"
type: session
status: current
updated: 2026-09-16
tags: [checkpoint, readiness, layering, ports, waiting, scaladoc, docs]
---

# Checkpoint: readiness into the domain

Three commits on `main`, plus the scaladoc sweep that preceded them. The design work is half done and the
remaining half is deliberately unshaped — this records the framing so it does not have to be re-derived.

## The stance the work is aimed at

> The current design put all the semantics (like awaiting ready / blocking) in the store. That is the
> design mistake: it pushed the semantics of the API surface all the way down to the implementation. A
> store is a store — it talks to the substrate and that's it. The blocking / waiting semantics are the
> responsibility of readiness. Stores remove their dependency on readiness and adopt a non-blocking
> semantics — "non-blocking" as in *no signal awaiting*.

Two things that framing settles, which the earlier `waiting-in-the-domain` note left open:

- Waiting is not "the use case's" or "a coordinating service's" — it is **readiness's own responsibility**.
  The readiness side owns *attempt → park on a signal → retry until the patience is spent*; the store is
  the thing it retries against.
- "Non-blocking" is narrow. An attempt still does a round trip, still costs time, can still queue for a
  connection. What it must never do is park waiting to be told something changed. That is why
  `Demand.patience` has no business in a store signature while `leaseTtl` and the batch size do.

## What landed

- **`79c8ff4` — readiness is a domain concern.** `QueueReadiness`, `LockReadiness`, `ReadinessProcessor`
  and `Wake` moved to `domain/service/readiness/` with a module in the house shape. None of the four needed
  an import changed, which is the evidence they had always been domain citizens. `Required` is
  `Consumer.Batched[ApplicationError.AdapterError, Wake]` — a toolkit contract plus a domain marker, so the
  domain names the seam without knowing a substrate exists (`Consumer` is covariant in `E`, which is what
  lets `WakeConsumer`'s `RedisFailure` satisfy it). The redis module's dependency reversed with it: it now
  *requires* the readinesses and *supplies* the wake path.
- **`ececb00` — `QueueStore.attemptClaim`.** The adapter's own private call, declared on the port. A spec
  pins the contract: a demand carrying 30 seconds of patience comes back empty in under a second. The
  blocking `claim` stays for now and routes through the same helper so the two cannot drift.

Verified at `79c8ff4`: 68/68 unit, 15/15 e2e standalone, 15/15 e2e cluster — the cluster run being the one
that matters, since the wake path is what only a real cluster exposes.

## What is next, and unshaped on purpose

Move the waiting out of `RedisQueueStore` so `claim` leaves the port, then the same for the lock. Three
things need deciding first, and none of them follows from the stance alone:

1. **One responsibility or two.** The queue waits for *any* work on a queue; the lock waits for *its turn*
   on a name, holding a ticket, with each park bounded by a delay the substrate named (`Asked.Wait(recheck)`).
   `QueueReadiness.awaitReady(queue, patience)(attempt)` is already the queue's whole loop bar the retry.
   The lock needs something the queue does not.
2. **The ticket's withdrawal.** Today `withdraw` runs from the store's `onExit` — patience elapsed, failure,
   interruption. Whoever owns the loop must guarantee it on *every* exit path, or a dead waiter holds the
   head and delays everyone behind it until its deadline. This is the one place a slip is a real stall.
3. **Subscribe before enter.** The mailbox must exist before the enter script runs. Inside one file that is
   a detail; across two components it is a contract, and it should be structural — the readiness wrapping
   the attempt — rather than remembered.

`Demand` carrying a patience the store ignores is the miniature of the whole problem, and it resolves when
the loop moves: the store's input narrows, `claim` disappears, and the non-waiting call can take the plain
name back.

## Also worth knowing

- `gracefulShutdownTimeout` stays at `Duration.Infinity` — deliberate, the real guard is k8s. There is no
  manifest in this repo yet, and `max-wait` defaults to 30 seconds, which is also Kubernetes' default grace
  period: a pod carrying an in-flight long-poll can be SIGKILLed at the moment it would have drained. When
  the manifest lands, that number wants to be above `max-wait`.
- All four `forkScoped` loops in `main` now carry `.interruptible`, which closes the uninterruptible-acquire
  trap from 2026-09-15 for every caller rather than for the one spec that found it.
- `'''` appears nowhere in `modules/server/src/main`; the rule is in `CLAUDE.md` and greppable on a diff.

## Parked (mechanical)

**The store's input still carries a patience it ignores.** `attemptClaim(demand: Demand)` answers about now,
and `Demand.patience` is documented as not consulted — but it is still in scope for a store, which is the
miniature of the inversion this work removes. Narrowing the input (the queue and the batch, or a type that
carries only those) is mechanical and deliberately deferred: it is churn against a refactor whose subject is
elsewhere. Do it when the lock side settles, so both ports narrow under one decision.

**The queue side is otherwise closed.** `QueueStore` is five operations, each answering about what is true
now; the waiting is `DequeueUseCase.claim`, parking on `QueueReadiness`; `DequeueUseCaseSpec` covers the
loop with no container.

