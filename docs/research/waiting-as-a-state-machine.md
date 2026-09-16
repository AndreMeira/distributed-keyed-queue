---
title: "Waiting as a state machine — what the queue and the lock would look like stated as transitions"
type: research
status: draft
updated: 2026-09-16
tags: [waiting, state-machine, lock, tickets, readiness, patience, expressivity, interruption]
---

# Waiting as a state machine

Both of DKQ's waits are state machines written as recursions. The queue's is small enough that nobody
notices; the lock's is not — it carries a ticket, a recheck deadline and a patience budget, and it can lose
its place and start again. This note asks what changes if the machine is *stated* rather than implied, what
that buys, and the one thing it cannot express.

Written after the waiting moved out of the stores and into the use cases (2026-09-16), when the loops became
readable enough to argue about.

## The lock's machine, stated

Two live states and two terminal ones. The store's answers are the events; the clock supplies the rest.

| state | carries | meaning |
|---|---|---|
| `Entering` | `within: Duration` | about to ask for the lock, with this much patience left |
| `Queued` | `ticket: Ticket`, `recheckAt: Instant` | holding a place, next deliberate ask due then |
| `Held` | `hold: Hold` | terminal: the lock is this caller's |
| `Spent` | — | terminal: the patience ran out |

Transitions. `left` is what remains of the patience, always measured from the instant the call arrived, never
renewed; `floor` is the 10ms minimum on any delay the store names, so a clock-boundary refusal cannot spin.

| from | event | guard | to |
|---|---|---|---|
| `Entering(within)` | `enter → Granted(hold)` | — | `Held(hold)` |
| `Entering(within)` | `enter → Queued(ticket, recheck)` | — | `Queued(ticket, now + max(recheck, floor))` |
| `Queued(t, at)` | patience check | `left ≤ 0` | `Spent` |
| `Queued(t, at)` | park elapses or a wake arrives, then `grant → Granted(hold)` | — | `Held(hold)` |
| `Queued(t, at)` | … `grant → Wait(delay)` | — | `Queued(t, now + max(delay, floor))` |
| `Queued(t, at)` | … `grant → Gone` | `left > 0` | `Entering(left)` |
| `Queued(t, at)` | … `grant → Gone` | `left ≤ 0` | `Spent` |

The park before each ask is `min(left, at − now)`, and a wake in the mailbox cuts it short. That is the whole
of "waiting": every ask follows a wake, a deadline the store named, or the patience running out.

Two properties fall out of the table that are hard to see in the recursion:

- **It terminates.** `left` is monotonically decreasing and every cycle either reaches a terminal state or
  parks for a positive duration bounded by `left`. There is no path that loops without consuming patience.
- **`Gone` is the only state regression**, and it costs a place in the queue but never time: the caller
  re-enters with `left`, not with its original patience.

## The queue's machine, for contrast

The same shape with one live state, which is why nobody felt the need to name it:

| state | carries | |
|---|---|---|
| `Looking` | `left: Duration` | attempt a claim; `Some` → `Held`, `None` → park on the queue's readiness |
| `Held` / `Spent` | | terminal |

The difference is that the queue's caller holds nothing between attempts. The lock's caller holds a *ticket*,
and that is the entire source of the extra states — and of the problem below.

## What stating it buys

- **The transition function becomes testable without a substrate.** `(state, answer) → state` is pure. Today
  the equivalent coverage needs a real store, because the transitions only exist as control flow between
  effects. Property tests over the table — patience never increases, `Gone` never resets the clock, no cycle
  without a park — would be cheap and would hold for both machines.
- **It documents itself.** The table above took an hour to recover from code that three people had already
  read carefully. That is a sign the machine deserves to be written down whether or not the code changes.
- **It generalises.** The two machines differ in their live states, not their skeleton: attempt, park until
  the next known event bounded by the patience, attempt again, give up when the budget is gone. A readiness
  that owned that skeleton — parameterised by what an attempt answers — would make "waiting" one thing in
  this codebase instead of two.

## What it cannot express, and why the code does not match the table

A transition table has no notion of *an abnormal exit*. The lock's `Queued` state owns a resource — a ticket
in the store's waiters list — and when the caller is interrupted, that ticket must be given up. There is no
event for "the caller vanished": interruption is not a transition, it is the absence of further transitions.

The current code handles it structurally, with one `acquireReleaseExit` per ticket:

```
entering  →  await (the ticket's scope: withdraw unless the wait ended holding the lock)  →  awaitTurn
```

Nesting is what gives a ticket a lifetime. A single flat loop over the state enum — the most faithful
expression of the table — puts the ticket *inside* the loop's state, where a bracket around the loop cannot
see which ticket is current, and the finaliser needs a mutable cell to read it.

The cost of dropping the finaliser is bounded but real: an abandoned ticket lives until its own deadline,
which is the moment its owner would have given up anyway, and the granting script prunes expired tickets from
the head. So it is a latency regression for the waiters behind — up to the abandoned caller's remaining
patience — rather than a correctness bug. It is asserted against in `LockAcquireUseCaseSpec`: *"a waiter
interrupted mid-wait gives up its place, so the next one is not delayed"*.

**So the tension is:** the state machine is the better description, and the nesting is the better
implementation, and the two do not want the same shape.

## Three ways to write it, with what each concedes

| | shape | concedes |
|---|---|---|
| **recursion** (today) | `entering` → `await` → `awaitTurn`, ticket and recheck as parameters | the machine is spread across three methods and must be reassembled by a reader |
| **two `Loop`s** | outer state `Duration` (one iteration per ticket), inner state `Instant` (one per ask), bracket between them | the machine is still in two pieces, though each piece is now explicit |
| **one `Loop`, state enum** | `Entering(within) \| Queued(ticket, recheckAt)`, every transition in one match | the ticket's lifetime, unless a `Ref` carries the current ticket to the finaliser |

`Workflow` from the toolkit maps well in one place — its `Step.Init` is exactly `Gone` ("start over") — and
its `persisted(store)` answers a failure a bracket cannot: a process that dies runs no finaliser, while
checkpointed state survives to be recovered. That is worth separating from the question of expressivity.

It is set aside here for a specific reason rather than a general one: **the ticket already is the durable
state.** It lives in the store's waiters list, carries the caller's deadline, and the granting script prunes
expired ones from the head — a recovery process in the protocol rather than in the runtime. Checkpointing
the waiter's state as well would write it once per park-and-ask cycle to replace something that costs
nothing. What is missing here is only the local half: releasing the ticket when the caller is cancelled
while this process is still alive. See `homelab-toolkit-zio`'s `docs/research/scoped-loop.md`.

## Worth trying

A fourth shape nobody has sketched: keep the flat state enum, and make the **state itself** the resource —
so the transition that leaves `Queued` is the transition that withdraws the ticket, and interruption is
handled by the scope that owns the state rather than by a bracket around the loop. That would need a loop
combinator that can finalise its own state, which the toolkit's `Loop` does not have and could gain:

```scala
Loop.scoped(initial)(step)(release: (S, Exit[E, O]) => UIO[Any])
```

If that reads well for the lock, it probably reads well for anything that waits while holding something —
which is the shape of every ticketed or leased wait, not just this one.
