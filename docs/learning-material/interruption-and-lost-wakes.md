---
title: "Interruption, returned values, and the wake that vanishes"
type: learning-material
status: current
updated: 2026-09-04
tags: [zio, interruption, fibers, concurrency, wakes, debugging]
---

# Interruption, returned values, and the wake that vanishes

A CI failure in `WaitersSpec` — *"a wake is never lost when a caller dies around it"* — turned out to be a
real hole, and closing it needed a property of ZIO interruption that is easy to state and easy to miss:

> **A value returned from an uninterruptible region to a fiber that has already been asked to die is
> discarded, and no finalizer in the effect chain sees it happen.**

Anything that hands ownership to a fiber and takes it back as a *returned value* inherits that hole. In dkq
the thing being handed over is a wake — "a key on this queue became claimable, go and look" — and losing
one means a consumer sleeps beside work it asked for.

## The mechanism, in three experiments

### 1. Interruption inside a mask is deferred, not dropped

`ZIO.uninterruptibleMask` lets a region declare that it may not be killed mid-way. Interruption requested
during that region is *recorded* and delivered when the region ends:

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
never runs — a distinction that showed up immediately in measurement: the same fix moved from 3 losses in
2,000 rounds to 0 by moving one line inside the mask.

## Why this is a lost wake, not a lost dequeue

`Waiters.waitFor` answers `true` when a wake was taken. Two ways of taking one:

- from `pending` — a wake that arrived with nobody waiting, taken at registration;
- from a delivered promise — a wake handed to this caller while it was parked.

Both mark the wake as consumed *before* the caller can return it. If the caller is interrupted in between —
a client disconnecting, a deadline elapsing, the pod shutting down — the wake dies with it. The key is still
in `ready`; no other waiter was told; nothing else is going to say so. The consumers on that instance sleep
out their patience beside claimable work, which is precisely the latency the doorbell design exists to
avoid.

Note the shape of the problem is not specific to `Waiters`. The same gap sits between `waitFor` answering
and `RedisQueueStore.claim` acting on the answer. **Any** repair local to the dying fiber is repairing one
link of a chain that has the same flaw at every link.

## The two-part fix

**Hand it back while you still can.** `Waiters.surrender` runs inside the mask, checks `interrupters`, and
rings the doorbell again if this caller took a wake it will never return. A wake means "look again", so a
spurious one costs a look, never correctness. This catches every interrupt requested before the check —
which is nearly all of them, because the caller spends its time parked in `await` or queued on the state
ref:

| shape | without `surrender` | with `surrender` |
|---|---|---|
| woken while parked, then interrupted (the CI test) | fails ~1 run in 100 | 0 in 15,000 rounds |
| wake taken from `pending`, interrupted immediately | 11 in 20,000 | 1 in 20,000 |

**Then let somebody outside notice.** The residual — interruption requested after the check, in a window a
few instructions wide — cannot be closed from inside the fiber, by the argument above. So `WakeListener`
does it from outside: a listening turn that heard nothing rings every queue somebody is parked on. A lost
wake then costs a wait of `DKQ_WAKE_BLOCK` rather than lasting until the queue next sees traffic.

The backstop only fires while the doorbell is silent — that is, while the queue is idle — so it costs
nothing when there is work. Measured back to back over the throughput sweep, the two versions are
indistinguishable.

## The rule to take away

If a fiber can be interrupted, do not model handover as *take, then return*. Either:

- make the transfer a state change that a finalizer can undo (`Waiters.settle` does this: it hands the wake
  on inside the same `modifyZIO` that removes the registration, so interruption of the wait is safe); or
- accept that a returned value can vanish, and put a bounded external check behind it.

What you cannot do is close it with a finalizer, however carefully attached. The value is gone before any
finalizer is asked.

## Where to look in the code

- `Waiters.waitFor` / `Waiters.surrender` — the mask, and the hand-back while the fiber can still act
- `Waiters.settle` — the interruption-safe half, where the transfer is state rather than a value
- `WakeListener.wake` — the silent-turn backstop
- `WaitersSpec` — the interleaving invariant, 200 rounds of racing a wake against an interrupt
- `QueueStoreSpec`, *"a consumer parked while the doorbell is silent is roused anyway"* — the backstop
