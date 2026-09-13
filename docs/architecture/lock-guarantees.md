---
title: "What the lock guarantees — the invariants a holder and a waiter may rely on"
type: architecture
status: current
updated: 2026-09-11
tags: [lock, guarantees, invariants, fencing, fairness, lease, contract, testing]
---

# What the lock guarantees

*How it works is [`lock-mechanics.md`](lock-mechanics.md); the same mechanism as concrete Redis state, call by call, is [`../learning-material/lock-state-walkthrough.md`](../learning-material/lock-state-walkthrough.md).*

The contract of the `KeyedLock` API, in terms a caller can design against and a test can assert. **No
implementation appears here** — nothing below depends on which store is behind it, and every statement
should survive a change of substrate. The queue's contract is [`guarantees.md`](guarantees.md); the lock is
its distilled core — exclusivity and the fence without messages or ordering — and the two documents share
their hardest truth (E2 there, M2 here).

## The one-sentence version

**At most one holder per lock, granted to waiters in the order they asked, with a fencing token that makes
a stale holder harmless downstream** — at the cost of the caller stamping its writes with that token, and
of "the order they asked" meaning the order the service saw.

## Vocabulary

- **Lock** — a name. Locks with different names share nothing.
- **Hold** — one caller's exclusive possession of a lock, valid until its lease expires unless refreshed.
- **Receipt** — the opaque handle a grant returns; the only way to release or refresh the hold.
- **Fence** — the number a grant returns beside the receipt. Strictly larger on every later grant of the
  same lock.
- **Patience** (`max_wait`) — the longest an acquire is willing to wait for a grant.
- **Grace** — the configured window after lease expiry in which a holder nobody displaced may still act.

---

## Mutual exclusion

**M1. At most one live hold per lock.** While one caller holds a lock, every other acquire waits or is
refused, and every release or refresh sent by anyone else reports failure and changes nothing.

**M2. Authorisation ends with the lease, and a holder must stop with it.** The service revokes the hold
and refuses the old holder's release and refresh. What it **cannot** do is stop that holder's code: a
process stalled past its lease — a long pause, a partition — may still be executing "protected" work while
the next holder starts. Nothing the service can do reaches inside a process it cannot talk to. Who owes
what:

- **The service owes:** never grant a lock while a live hold exists, and never accept release or refresh
  from a hold that has been displaced.
- **The holder owes:** refresh before the lease ends, and stop the moment a refresh reports the hold lost.
- **For effects outside the lock** — the write the lock was protecting — neither of the above is enough,
  because the stalled holder's write lands before anyone notices. That is what the fence is for.

**M3. The fence makes M2's gap harmless, and it is the only thing that does.** Fences for one lock strictly
increase across grants: whoever holds later carries a larger number. A holder that stamps its protected
writes with its fence, against a downstream that remembers the largest fence it has seen and rejects
anything smaller, gets exclusivity end to end — the stale holder's write arrives with yesterday's number
and is refused. A caller that ignores the fence has mutual exclusion only between well-behaved processes,
which is to say: not when it matters.

**M4. Fences order; they do not count.** Consecutive grants of one lock may receive non-consecutive
numbers. Only the comparison may be relied on, and only between fences of the same lock.

## Acquisition and fairness

**A1. An acquire is a bounded attempt, not a queue position to keep.** It ends in a grant, or in an
ordinary "not acquired" when the patience runs out. A held lock is never an error — waiting its patience
for a busy lock is the call behaving exactly as asked.

**A2. Grants follow arrival order, among waiters still within their patience.** Arrival is when the
service serialises the acquire — two acquires racing from different clients are ordered by which the
service saw first, and by nothing else. There is no earlier "first" to appeal to: without a shared clock,
the service's order is what "first" means.

**A3. A waiter that gives up leaves the queue.** Patience running out, or the caller abandoning the call,
surrenders the place; those behind move up and nothing else about their order changes.

**A4. No barging.** A newcomer cannot be granted past anyone already waiting within patience — however
small its own patience, it joins the tail. There is no call that jumps the queue: a free lock with waiters
is, to everyone but the head waiter, a held lock.

**A5. A dead holder delays, but cannot orphan, the lock.** Once its lease expires, the next grant goes
ahead — to the head waiter, or to the next acquire to arrive — with a larger fence. Recovery is bounded by
the lease, and needs nothing from the dead holder: no identity re-established, no cleanup performed.

## Holding

**H1. A hold is valid until its lease ends, and refresh extends it.** The grant says when the lease
expires; each successful refresh answers with the new expiry. A ttl beyond the service's ceiling is
clamped, not refused — the response's expiry is authoritative, and a hold that must outlast the ceiling
refreshes.

**H2. Late is not lost — within the grace.** A holder whose lease lapsed but whom nobody displaced may
still refresh, up to the configured grace past expiry. Beyond the grace the hold may be removed at any
moment, so a holder that has been away longer than the grace must treat itself as having lost the lock,
whatever its next refresh says.

**H3. Release reports whether it applied, and lying is impossible.** `released = false` means the hold had
already ended — displaced, or released before. Releasing twice is safe and reports false the second time. A
release from a displaced holder changes nothing for the current one.

**H4. A refused receipt is an answer, not an error.** A receipt the service never issued is rejected as
invalid input. A receipt whose hold has ended reports `released = false` or `renewed = false`. Only a
malformed request or an unavailable store is an error.

## What is *not* guaranteed

- **Not reentrant.** A holder that acquires its own lock again is a second waiter like any other: it queues
  behind itself and waits its full patience for a hold it can never get. Nothing detects this.
- **No deadlock detection.** Two callers each holding one lock and waiting for the other's will both wait
  out their patience. Lock ordering discipline belongs to the caller.
- **Not exclusive against code that outlives its lease** — see M2. The service refuses the stale holder's
  *calls*; only the fence (M3) refuses its *writes*.
- **No priority.** The only order is arrival order.
- **No promptness promise for the grant after a release** — only that a waiter within patience is granted
  in its turn, and that the wait is bounded by patience. In practice a release reaches the head waiter
  quickly; do not build on "quickly".
- **No cross-lock ordering.** Grants of different locks happen in no promised relation to each other.
- **Fence numbers say nothing between locks** — see M4.
- **Durability is the store's, not the service's.** A store that loses its data loses the locks with it;
  holders learn this through refreshes reporting the hold lost (or, past the grace, through H2).

## Writing tests against this

- **Anything about exclusion (M) needs two callers**, and needs the second to *try* while the first holds.
- **M2 is only half testable.** That a displaced holder's release and refresh are refused is easy. That its
  code stopped is not observable from outside — that half is a code review of the caller, not a test.
- **M3 is testable without any downstream**: acquire, let the lease lapse, let another acquire — the second
  fence must exceed the first. The downstream's rejection of the smaller fence is the *downstream's* test.
- **Fairness (A2) needs at least three waiters** with arrival order fixed deliberately — pauses between
  acquires long enough that the service cannot have seen them in another order. Two waiters cannot
  distinguish "in order" from "by chance".
- **A4 needs the barger to keep trying through a handover** — a single attempt at a chosen moment tests
  only that moment. Spam minimal-patience acquires across a release and assert none wins while a waiter is
  queued.
- **A5 and H2 need a real clock and a real lease**, short enough not to slow the suite, long enough that a
  slow machine cannot expire it mid-step and turn a correctness test into a flake.
- **H2's two halves pull opposite ways**: within-grace-may-refresh needs a generous grace, beyond-grace-is-
  gone needs a tiny one. They are two tests, not one.
- **Prefer asserting through the API.** A test that inspects the store asserts an implementation and fails
  for the wrong reason when it changes.
