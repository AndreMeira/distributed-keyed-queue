---
title: "A wait written as states — defunctionalised recursion, absorbing terminals, trampolined"
type: architecture
status: current
updated: 2026-09-17
tags: [lock, waiting, state-machine, defunctionalisation, trampoline, interruption, patterns]
---

# A wait written as states

`LockAcquireUseCase` writes its wait as a sealed set of state classes with a transition on each, driven by
a small runner. The shape reads well and sits on no single named pattern, which makes it hard to look up
and hard to reuse. This page names what it is made of, so the abstraction can be hunted for deliberately
rather than rediscovered.

The code is `domain/service/usecase/lock/LockAcquireUseCase.scala`. Why the wait is a machine at all —
which states exist, what each transition means, what the machine cannot express — is
[waiting-as-a-state-machine](../research/waiting-as-a-state-machine.md). This page is only about the shape
that expresses it.

## The shape

```scala
sealed private trait AcquireLifecycle:
  def next: IO[AdapterError, AcquireLifecycle] = ZIO.succeed(this)

private object AcquireLifecycle {
  case object GivenUp extends AcquireLifecycle
  
  case class  Granted(hold: LockStore.Hold) extends AcquireLifecycle
  
  class Placing(waiter: Waiter, within: Duration) extends AcquireLifecycle:
    override def next = ??? // ask; granted, or take a ticket
  
  class Queued(waiter: Waiter, ticket: Ticket, recheckAt: Instant) extends AcquireLifecycle:
    override def next = ??? // park, ask, and land somewhere

  private def loop(state: AcquireLifecycle): IO[AdapterError, Option[LockStore.Hold]] =
    ZIO.uninterruptibleMask: restore =>
      restore(state.next).flatMap:
        case Granted(hold) => ZIO.succeed(Some(hold))
        case GivenUp       => ZIO.succeed(None)
        case next          => restore(loop(next))
}        
```

Three known things are composed here. Each has a name; the combination does not.

## Defunctionalisation

This is the exact relationship between the shape above and the plain recursive function it replaced.

Defunctionalisation — Reynolds, 1972 — turns a higher-order or recursive control flow into a data type:
take every recursive call site, make each one a named constructor holding exactly the free variables live
at that point, and write one interpreter that dispatches on the constructor. That is what `Placing` and
`Queued` are. They are not a model of the lock invented alongside the code; they are the call sites of the
recursion, given names and forced to declare what they carry. `Queued(waiter, ticket, recheckAt)` is the
argument list of a recursive call that used to be spelled out in a `flatMap`.

This is why the shape reads better than the recursion it came from, and the gain is specific: recursion
hides its call sites in syntax, so the states exist but are never named and their payloads are whatever
happens to be in scope. Defunctionalising makes both explicit and lets a reader enumerate the states by
reading the sum type instead of tracing the control flow.

It is also why `next` is the natural place for a transition's own cleanup. `Queued.next` gives the ticket
back on every exit but the two that keep it — a rule about one call site, which now has somewhere to live.

## A Kleisli endomorphism, with absorbing terminals

`next` has type `S => F[S]`: an endomorphism in the effect's Kleisli category. It is not `S => F[Either[S, B]]`,
and that is the whole difference from the library abstractions below.

Termination falls out of the default. `GivenUp` and `Granted` do not override `next`, so they inherit
`ZIO.succeed(this)` and are **fixed points** of the transition — absorbing states, in the Markov sense:
entered, never left. The machine is total, every state has a transition, and "finished" means "reached a
fixed point" rather than "returned a different kind of thing".

The runner is what knows which states answer. That is the cost of this choice: `loop` pattern-matches on
`Granted` and `GivenUp` by name, so states and runner are coupled, and a new terminal state means editing
the runner. At this confinement — one private machine, four states, one driver, all in one file — that is
cheap and visible. It would not survive being a library.

## A trampoline

`loop` is the runner that iterates the coalgebra to its fixed point. It is `tailRecM` with the `Either`
replaced by subtype discrimination, and it is a trampoline in the ordinary sense: each step returns a
value describing the next step rather than calling it.

The flat finalizer stack is a property of the trampoline, not of the state machine. A wait that recursed
directly inside its own scope would accumulate a finalizer per iteration; bouncing through `loop` means
each step's handlers run and unwind before the next begins. The argument is in the research note under
*Why a loop and not plain recursion*.

`loop` also owns the interruption seam. A step is interruptible — a waiter parks inside one — and the
space between two steps is not, so a step's own handlers decide the fate of anything that step holds. Both
`restore` calls are load-bearing: the second is what keeps iterations after the first interruptible.

## Why no existing abstraction fitted

`Loop`, `Workflow` and `tailRecM` all make "continue vs done" a **wrapper**: `Next.Continue | Next.Done`,
`Step.Continue | Step.Done`, `Left | Right`. The state is re-wrapped at every transition, and the wrapper
is a second vocabulary sitting on top of the one the states already have.

This shape puts the choice in the state's own type instead. A transition returns a state, not a
state-in-a-box, and the two states that answer say so by being themselves. That is the single degree of
freedom taken here, and it is why the toolkit's `Loop` was tried and dropped — see
`docs/research/scoped-loop.md` in `homelab-toolkit-zio` for the other half of that attempt, which is about
a loop releasing its own state.

## Follow-up — this needs abstracting properly

The shape is currently a private machine inside one use case, and it stays that way until someone works
out what it generalises to. Two things are already known to be wrong for it: `Loop` and `Workflow`, for the
wrapper reason above, and `Stateful`, which separates behaviour from state where here the state *is* the
behaviour.

What an abstraction has to keep:

- **transitions typed `S => F[S]`**, with no wrapper, and terminals as fixed points of the default;
- **a seam the runner owns**, not the states — interruptible step, uninterruptible gap;
- **per-state cleanup that runs at the step boundary**, so a transition can hold something and decide its
  fate on every exit;
- **a flat finalizer stack** across an unbounded number of steps.

What is open:

- how a runner learns which states answer without pattern-matching them by name, given they are subtypes
  rather than a wrapper — a `terminal: Option[B]` on the trait, a second sealed layer, or a typeclass;
- whether the states can stop being inner classes. They close over `store` and `readiness` from the
  enclosing use case, which is what makes the machine unliftable today;
- whether the runner's result type belongs to the machine or to the caller;
- whether the queue's wait — a simpler machine, described in the research note — has the same shape once
  written this way, which is the test of whether there is an abstraction here or one good fit.

Until that is answered the duplication between the lock's machine and any future one is accepted; a second
instance is the evidence needed to pick between the options above.
