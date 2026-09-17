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

## Open: how the lock's wait should be expressed

The lock's waiting now lives in `LockAcquireUseCase` with no mutable state: a `Waiter` carries what does not
change (acquisition, asked, mailbox), and the ticket and the recheck instant are parameters of the
recursion. Three shapes were weighed for what comes next; none is obviously right, and the current one is
good enough to leave alone.

1. **The recursion as it stands.** `entering` → `await` (per-ticket `acquireReleaseExit`) → `awaitTurn`. The
   ticket has a scope, so withdrawal on interruption is correct by construction. The machine is spread over
   three methods.
2. **Two `Loop`s** (`homelab.common.flow.Loop`, `Next.Continue | Next.Done`). Outer loop: one iteration per
   ticket, state = the patience left (`Duration`). Inner loop: one iteration per ask, state = when the
   answer can next change (`Instant`). The per-ticket bracket sits between them, which is where the ticket's
   lifetime belongs. Bodies stay named methods, partially applied — `Loop(patience)(entering(waiter))` — so
   no anonymous logic. Costs one indirection per loop and a curried definition.
3. **One `Loop` with a state enum** — `Entering(within) | Queued(ticket, recheckAt)`. The best *description*
   of the machine: every transition in one match, and `Gone → Entering(left)` reads as what it is. But a flat
   loop changes the ticket inside itself, so a bracket around the loop cannot know which ticket to withdraw:
   it needs a `Ref` again, which is the sideways update this refactor removed.

**What the third option actually costs, measured rather than assumed.** Without the finaliser an interrupted
waiter's ticket lives until its own deadline — which is when that caller would have given up anyway, since
the ticket's deadline *is* its patience end, and the grant script prunes expired tickets from the head. So
it is a latency regression for the waiters behind, bounded by the abandoned caller's remaining patience, not
a correctness bug. `LockAcquireUseCaseSpec` asserts against it: "a waiter interrupted mid-wait gives up its
place, so the next one is not delayed".

`Workflow` was considered and set aside: it models resumable, keyed, possibly-persisted processes, and a
waiter's wait is none of those — `persisted` wants a `KeyValueStore` for state that dies with the process
anyway, and `serialised` wants a `KeyLock`, which is what the lock *is*. It would earn its keep if the
branching ever got genuinely complicated.

## The lock half

The same cut, on the harder side. `LockStore` answers what the Lua already answered and waits for nothing:

```
place(acquisition, within) -> Position.Granted(hold) | Position.Queued(ticket, recheck)
ask(acquisition, ticket)   -> Turn.Granted(hold) | Turn.Wait(delay) | Turn.Gone
withdraw(name, ticket)
```

Nine methods and the spin floor left `RedisLockStore`, which is script calls and codecs again. `Ticket`
joined `Token` in `domain/types`. The waiting is `LockAcquireUseCase`'s, as a four-state machine —
`Placing`, `Queued`, `Granted`, `GivenUp` — with `State.loop` in the enum's own companion driving it.

> The names here are what stood on the 16th. `State` and its companion's `loop` became the
> `AcquireLifecycle` trait and the `loop` beside it the next day, and the driver stopped narrowing its
> argument to the live states. The shape as it stands is
> [`architecture/states-as-classes.md`](../architecture/states-as-classes.md).

**What the vocabulary settled on, and why it changed twice.** `enter`/`Entered` paired with `grant`/`Asked`,
which crossed itself: the method was named for the outcome it hoped for and the result for the act, so
`grant` could answer `Asked.Gone`. `place`/`Position` and `ask`/`Turn` name where a caller stands and
whether its turn has come. The scripts keep `AcquireScript.Entered` and `GrantScript.Asked`, so the adapter
visibly translates the substrate's words into the port's.

**`subscribe` hands out a `Signal`, not a `Queue[Unit]`** — a waiter needs one capability, and the queue
handed it `offer`, `shutdown` and `size` as well.

### Three shapes were tried, and the third won

1. Nested `acquireReleaseExit` + a `Waiting` outcome enum. Correct, no interruption trickery, machine spread
   over three methods.
2. `Loop` from the toolkit with `Next.Continue | Next.Done`, plus a hand-rolled guarded loop.
3. One flat machine whose `State` includes the terminal cases, so a step is `State => IO[E, State]` and the
   driver exits on `Granted`/`GivenUp`. `loop` takes `State.Placing | State.Queued`, so "a step starts from
   a live state" is a type rather than a comment.

Two findings came out of building them, both recorded in `docs/research/waiting-as-a-state-machine.md` and,
from the toolkit's side, in `homelab-toolkit-zio`'s `docs/research/scoped-loop.md`:

- **The naive guarded loop is wrong and fails silently.** `restore` must wrap the recursive call as well as
  the step; without it every iteration past the first is uninterruptible, `timeout`-based parking never ends
  and `interrupt` blocks. The symptom was two tests hanging for a minute each, with no error.
- **A loop beats plain recursion for a reason unrelated to interruption.** Mutual recursion never discharges
  a handler until the whole wait ends, so finalisers accumulate one per transition: a waiter that parks 300
  times pays 300 withdrawals when cancelled. Neither version looks different on the page.

## Two traps worth remembering

**A corrupt TASTy file looks like a dozen unrelated compiler bugs.** `LockStore.tasty is broken, reading
aborted with IndexOutOfBoundsException` finally named what had been appearing all day as
`NoClassDefFoundError` on `Module$`/`KeyLayout$`/`QueueConfig$`, "type Message is not a member of v1",
"value Hold is not a member of LockStore", and one run of three phantom test failures. `sbt server/clean`
cleared it in seconds.

This also revises an older entry: the **unexplained intermittent** in `RedisLockStoreSpec` that
`2026-09-13`'s log has carried across three sightings — six failures on a full-suite run, passing alone,
passing on a re-run — is much more likely to have been this than a race. If it recurs, clean the module
before hunting for a timing bug.

**An unused constructor parameter is the one dependency the compiler will not flag.** Both stores kept a
`readiness` they had not used since the waiting moved out, through three commits of a refactor whose whole
point was removing exactly that dependency. Nothing failed; the import and a `@param` line kept it alive.
`-Wunused:params` would catch it, at the cost of noise elsewhere. Until then it is a review question: after
moving logic out of a class, check what its constructor still asks for.

