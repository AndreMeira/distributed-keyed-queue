---
title: "Interruption, returned values, and the wake that vanishes"
type: learning-material
status: current
updated: 2026-09-04
tags: [zio, interruption, fibers, concurrency, queues, wakes, debugging]
---

# Interruption, returned values, and the wake that vanishes

A CI failure in `WaitersSpec` — *"a wake is never lost when a caller dies around it"* — turned out to be a
real hole, and the search for a fix ended in a different design. The property underneath is easy to state
and easy to miss:

> **A value returned from an uninterruptible region to a fiber that has already been asked to die is
> discarded, and no finalizer in the effect chain sees it happen.**

Anything that hands ownership to a fiber and takes it back as a *returned value* inherits that hole. Here
the thing handed over was a wake — "a key on this queue became claimable, go and look" — and losing one
meant a consumer asleep beside work it had asked for.

## The mechanism, in four experiments

### 1. Interruption inside a mask is deferred, not dropped

`ZIO.uninterruptibleMask` lets a region declare it may not be killed mid-way. Interruption requested during
that region is *recorded* and delivered when the region ends:

```scala
ZIO.uninterruptibleMask: restore =>
  restore(gate.await).as(true) <* (inTail.succeed(()) *> ZIO.sleep(300.millis))
```

Complete `gate`, wait until the fiber is in the uninterruptible tail, then interrupt it. The effect computes
`true` and the tail runs to completion — but the fiber's exit is:

```
EXIT=Failure(Interrupt(...))
```

The work finished. The answer did not survive.

### 2. Finalizers do not see it

The obvious repair is a finalizer: if the value is being dropped, hand the wake back on the way out. It does
not work, and the reason is worth internalising — the finalizer runs against the *effect's* exit, which is a
success, while the *fiber's* exit is a failure:

```
fiberExit=Failure   outerFinalizerSaw=Success
```

`onExit`, `onInterrupt`, `acquireReleaseExit` — all of them see `Success`. The value is dropped in the gap
between the effect completing and the fiber completing, and nothing in the chain is invoked there. A
finalizer attached *outside* the mask does not help either: the deferred interrupt fires the moment the mask
ends, before the code after it runs.

### 3. What the fiber *can* see

A doomed fiber knows it is doomed, from the moment interruption is requested:

```scala
ZIO.descriptorWith(descriptor => ... descriptor.interrupters.nonEmpty ...)
```

That check has to sit **inside** the mask. Outside, the pending interrupt is delivered first and the check
never runs — a distinction that showed up immediately in measurement: moving one line inside the mask took
the same fix from 3 losses in 2,000 rounds to 0.

### 4. `queue.take.timeout(d)` drops elements

A ZIO `Queue` looks like the way out: `take` hands an element to exactly one taker, so let the queue own the
race. It does — until a deadline is added. Same race, two shapes, 20,000 rounds each:

```
masked take, no timeout      LOST=0      of 20,000
masked take + timeout        LOST=19,397 of 20,000
```

`timeout` forks the take into a **child fiber**, which the parent's mask does not protect. Interrupt the
parent and the child dies holding the element. So a `Queue` is safe as a handover only while the wait is
unbounded — which is why `homelab-toolkit-zio`'s `KeyedQueue` is safe: its `takeWith` never times out, and
it acts on the key inside the mask rather than returning it.

The rule this leaves: **a timed wait needs an atomic decision between "I was given it" and "I gave up".** A
`Promise` has that CAS — `succeed` and `interrupt` each report whether *they* completed it. A `Queue` does
not expose one.

## Three designs, and where each put the half-finished state

The hole is not really about ZIO. It is about a handover having an instant where the thing is neither here
nor there, and what happens if the holder dies in that instant. dkq has answered it three ways:

| | blocking (`BLMOVE`) | doorbell + registry | broadcast bell |
|---|---|---|---|
| half-finished state | `claiming:<worker>` list, **in Redis** | a wake in a fiber's hands, **in memory** | none |
| hand-back | `LMOVE claiming → ready` — positional, asks nothing about the outcome | `surrender` — must ask ZIO whether it is dying, and in a narrow window the answer comes too late | not needed |
| swept recovery | `watchdog.lua` sweep 2, over durable state | listener backstop, over in-memory `parked` | not needed |
| price | a `WorkerId`, a box per connection, a `workers` liveness set, a third sweep | two mechanisms guarding one handover | one look each per ring |

The blocking design was **immune** to everything in the four experiments above, and not by luck: `release`
never asked what had happened, it said *"drain my box, whatever is in it"* — a query against durable state
rather than a value that could be discarded. Its expensive machinery was buying exactly that durability.

Removing the machinery (a claim became one script, so there was no box to sweep) moved the same half-second
of ambiguity out of Redis and into fiber-local memory, where neither positional recovery nor sweeping is
possible. That is when the hole appeared, and the two in-memory mechanisms that defended it — `surrender`
and the listener backstop — were the old two ideas rebuilt, less exactly.

## What shipped: no handover at all

Each queue has a **bell**: a `Promise` for the current round. `subscribe` takes it, a ring completes it and
installs a fresh one, and everyone holding it wakes. Nothing is consumed, so no caller can take a ring from
another, and a caller that times out, is interrupted, or dies mid-claim removes nothing from the system.

The ordering is what makes it sound, and it is the one thing to keep right:

```
subscribe → attempt the claim → wait on the bell
```

A ring landing while the attempt is in flight completes the bell the caller is already holding. Look first
and subscribe after, and exactly that ring is lost. There is no `pending` flag to forgive it, and no test
can time the window — the ordering is documented at both ends instead.

The price is that every consumer waiting on a queue wakes per ring, and one of them wins. Measured against
the registry over the throughput sweep and the idle-consumer wake path, the difference is inside
run-to-run noise: the redundant attempts fall on consumers that are idle by definition, and a woken consumer
leaves the waiting set, so the fan-out does not compound under load.

## The rule to take away

If a fiber can be interrupted, do not model handover as *take, then return*. In order of preference:

- **Do not transfer ownership.** Signal instead — a broadcast takes nothing from anyone, so interruption
  cannot destroy it. This is the only option with nothing left to defend.
- **Act on it inside the same uninterruptible step** rather than returning it, as `KeyedQueue.takeWith`
  does. Sound, but it forbids a deadline on the wait.
- **Put the half-finished state somewhere durable and sweep it**, as the `BLMOVE` design did. Sound at any
  granularity, and the price is the bookkeeping.

What you cannot do is close it with a finalizer, however carefully attached. The value is gone before any
finalizer is asked.

## Where to look in the code

- `Waiters` — the bell, and `subscribe`'s contract
- `RedisQueueStore.pursue` — subscribe, look, wait, in that order and for that reason
- `WaitersSpec` — the semantics, plus 500 rounds racing a ring against an interrupt
- `docs/research/non-blocking-dequeue.md` — the design this replaced, and why
- `docs/learning-material/claiming-identity.md` — the `BLMOVE` design's box, and what it cost
