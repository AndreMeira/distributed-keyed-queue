---
title: "One shape for both readinesses: a scoped signal instead of a callback"
type: research
status: current
updated: 2026-09-27
tags: [readiness, queue, lock, interruption, scope, api-shape]
---

# One shape for both readinesses

`QueueReadiness` and `LockReadiness` answer the same kind of question — "something you were waiting for may
have happened" — and look nothing alike. Asked on 2026-09-27 whether the difference is earned. Partly: one
half of it is forced, the other half is not, and the unforced half is where the hardest code in the domain
lives.

## What differs today

| | `QueueReadiness` | `LockReadiness` |
|---|---|---|
| holds | one `Queue.sliding(1)` per queue name, shared | one mailbox per subscriber |
| a wake reaches | one consumer | every waiter on the name |
| API | `awaitReady(queue, patience)(claim)` — higher-order | `subscribe(lock)` → `Signal.await` |
| conservation | hand-written: `uninterruptibleMask`, two `restore`s, `onInterrupt`, `onExit` | none needed |
| the waiter | tail-recurses on patience | drives a `Recursion` over `Demanding`/`Queued`/`Granted`/`GivenUp` |

## What is forced, and what is not

**Forced: one token versus one mailbox each.** Any consumer can serve any key, so waking one is enough and
waking all would have every loser spend a round trip to be told there is nothing for it. A lock is the
opposite: grants go by ticket order, only the store knows whose turn it is, and — the part that closes the
question — a waiter must subscribe *before* it asks, so it has no ticket at subscription time and a wake can
never be addressed to the head. Broadcast is not a preference there; it is the only thing available.

**Not forced: the API shape.** `awaitReady` takes the claim as a callback so the token never reaches the
caller, "so it cannot be held as a value by a fiber that dies with it". That is a real hazard and the reason
given is sound — but a callback is not the only way to keep a resource from escaping. A scope is the other
way, and it is the one this codebase already uses everywhere else, `LockReadiness.subscribe` included.

**And the reason the lock needs no conservation at all is not in the architecture doc.** It is not that a
lock wake matters less. It is that `awaitTurn` parks for
`min(recheckAt - now, patienceLeft)` — a queued waiter re-asks the store on a schedule whatever happens, so
a lost wake costs latency up to the recheck and never liveness. `awaitReady` has no such floor: it waits for
a token and nothing else, so a lost token means a queue going quiet with work sitting in it. That difference,
not the thundering herd, is what forces the two conservation stories apart. `readiness-and-wake.md` should
say so.

## The alternative: a scoped signal

```scala
def subscribe(queue: QueueName): ZIO[Scope, Nothing, Signal]
```

where the `Signal` records whether this subscriber currently holds the token, `await(patience)` takes it, and
the scope's release re-offers it if it is still held. The four paths the current code handles by hand:

| | today | with a scoped signal |
|---|---|---|
| interrupted while waiting | `.onInterrupt(found.offer(()))` | release re-offers |
| the look found work | `onExit ⇒ offer` — hand on | release re-offers |
| the look found nothing | `onExit ⇒ unit` — keep it, which ends the chain | the next `await` blocks on an empty buffer |
| the patience expired | offer unconditionally, since a lost take is unknowable | `await` does it internally |

The third row is the one that looked like a problem and is not. I first thought the caller would have to
declare its outcome — "I looked and found nothing, do not put it back" — because a scope finalizer cannot see
the claim's result. But looping means calling `await` again, and `await` on an empty buffer blocks. The chain
ends for exactly the reason it ends today, without anyone having to say anything.

### What it deletes

The whole of the conservation interleaving in `awaitReady`: the `uninterruptibleMask`, both `restore`s, the
`onInterrupt` attached outside the timeout, the `onExit` discriminating `Exit.Success(None)` from everything
else, and the unconditional re-offer after a spent take. Three of the sharpest comments in the service exist
to explain why each of those sits exactly where it does. What replaces them is `ZIO.acquireRelease`, which is
what `LockReadiness.subscribe` already does.

Both readinesses would then have one shape — `subscribe` returning a scoped signal, the caller driving its own
loop — and the entire remaining difference would be one shared token versus one mailbox per subscriber, which
is the part that is forced.

### The two wrinkles

**Hand-on timing.** Today the token goes back the instant work is found, before the claim returns. Under
scope-release it goes back when the scope closes. In `DequeueUseCase` that is immediately afterwards, so the
gap is small — but it is a gap, and under a burst the next consumer waits for a scope to close rather than for
work to be found. A throughput question, not a correctness one, and worth measuring rather than assuming.

**A timed-out take is still unknowable.** The existing comment is right: when the timeout discards an element
the take itself was never interrupted, so nothing observes the loss. This does not go away with a scope, which
means `await` must take the patience and do the conservative re-offer itself rather than leaving the caller to
write `signal.await.timeout(p)`. That keeps the guard inside the readiness, where it belongs, and is the one
place the two signals' `await` would differ in signature.

## The use cases stay different, and that part is earned

`DequeueUseCase.claim` tail-recurses; `LockAcquireUseCase.acquire` drives a `Recursion` over four named
states. That is not stylistic. A lock waiter carries a **ticket** between attempts: a resource that can be
lost (`Turn.Gone` sends it back to `Demanding`) and that must be withdrawn when the wait ends badly. A queue
consumer carries nothing between looks but a deadline. A state machine there would have one state.

So unifying the readiness API does not imply unifying the use cases, and should not be read as an argument for
it.

## What would have to be true to do it

1. A `Signal` that tracks whether it holds the token — a `Ref[Boolean]`, set on a successful take, cleared when
   re-offered. Local and small.
2. `await(patience)` inside the readiness, so the timed-out-take guard stays there.
3. A measurement of the hand-on gap under a burst, since that is the one behaviour that genuinely changes.
4. The `sliding(1)` buffer makes double-offer harmless — re-offering into a full buffer keeps one — so the
   failure mode of an over-eager release is a spurious token, which costs one wasted look and cannot
   accumulate. That asymmetry is already the stated design.

## Why this was not obvious

The architecture doc argues `## One token, one consumer` on thundering-herd grounds, and that argument is
correct — for the *routing* question. It then reads as though it also justifies the callback API, which it does
not: the callback exists for resource safety, and the lock avoids needing resource safety through a polling
floor the doc never mentions. Two independent decisions presented as one is what made the asymmetry look
accidental.
