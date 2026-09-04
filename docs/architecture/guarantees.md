---
title: "What dkq guarantees — the invariants a consumer may rely on"
type: architecture
status: current
updated: 2026-08-30
tags: [guarantees, invariants, ordering, delivery, contract, testing]
---

# What dkq guarantees

The contract, in terms a caller can design against and a test can assert. **No implementation appears
here** — nothing below depends on which store is behind it, and every statement should survive a change of
substrate. If one does not, either the substrate is wrong or this page is.

Two audiences: someone writing a producer or consumer and deciding what their code must handle, and someone
writing tests, for whom each invariant below is meant to read as an assertion.

## The one-sentence version

**A key is worked by one consumer at a time, its messages are handed out in the order they were sent, and
nothing is lost when a consumer dies** — at the cost of at-least-once delivery, and of ordering that is
about *handing out*, not about the order effects land in.

## Vocabulary

- **Queue** — an address. Messages sent to different queues share nothing.
- **Key** — what ordering and exclusivity are defined by. Chosen by the producer; every message carries one.
- **Message id** — a message's name, unique among the messages currently queued for its key.
- **Claim** — a consumer's exclusive hold on a key, covering one or more of its messages. Identified by a
  **receipt**, and valid until its lease expires.
- **Settle** — reporting what became of a message: acknowledged (done with it) or nacked (not done with it).

---

## Ordering

**O1. Messages for one key are handed out in the order they were enqueued.** For a single key, a consumer
receives message *n* before message *n+1*, on every delivery, including redeliveries.

**O2. A message that is not acknowledged keeps its place.** Nacking, or dying, does not move a message
behind later ones. Its next delivery is still ahead of everything sent after it.

**O3. Ordering is per key and nothing else.** No ordering is promised between different keys, between
queues, or between messages a producer sent concurrently without keying them together.

**O4. Order is about delivery, not about effects.** A consumer holding several messages may acknowledge
them in any order, and may nack one and acknowledge the next. The nacked message is then retried *after*
the later one already ran. If effects for a key must land in order, the consumer must stop at its first
failure — the service will not stop for it.

## Exclusivity

**E1. At most one consumer is authorised to work a key at a time.** While one holds a key, no other
receives any message for it. A consumer that loses its claim — by letting the lease lapse, or by being
declared dead — is no longer authorised, cannot reclaim what it was holding without a fresh claim, and its
settles are refused.

**E2. Authorisation ends the moment the lease does, and a consumer must stop with it.** dkq revokes the
claim and refuses everything the old holder sends. What it **cannot** do is stop that consumer's code from
running: a process that has stalled past its lease — a long pause, a partition — may still be executing a
handler while a new consumer starts on the same key. Nothing the service can do reaches inside a process it
cannot talk to.

This is the one place where "one at a time" depends on the consumer as well as the service, so it is worth
being exact about who owes what:

- **dkq owes:** never hand a key to a second consumer while the first is authorised, and never accept work
  from a consumer that is not.
- **The consumer owes:** stop when it can no longer renew. A handler that outlives its lease must abandon
  what it is doing rather than finish and settle.
- **For effects outside dkq** — a database write, a call to another service — neither of the above is
  enough on its own, because the stalled consumer's write lands before anyone notices. Pass the receipt to
  that system as a fencing token and have it reject a stale one. That is the only way to make "one at a
  time" true end to end.

**E3. A claim covers messages, not the whole key's future.** Messages enqueued for a held key are accepted
and wait; they are not part of the live claim, and the holder may or may not see them.

**E4. Within one claim, working one message at a time is the consumer's job.** A consumer that asks for
several receives several, and nothing prevents it working them concurrently. The service guarantees only
that no *other* consumer has that key. A consumer that parallelises inside its own batch breaks per-key
serialisation, and nothing will report it.

**E5. Nothing is promised about which consumer gets a key**, or in what order keys become claimable
relative to each other.

## Delivery

**D1. At least once.** Every enqueued message is delivered at least once, unless it is acknowledged first.

**D2. Never silently lost.** A message stays queued until it is acknowledged. A consumer that crashes,
stalls, or is partitioned away loses only the acknowledgements it had not sent.

**D3. Duplicates are possible and must be tolerated.** A consumer may finish its work and die before
acknowledging; the message will be delivered again. Handlers must be idempotent, or accept re-running.

These duplicates are *sequential* — the second delivery happens after the first consumer stopped being
authorised. The one case where two consumers can be running handlers at the same moment is E2, and it needs
one of them to have stalled past its own lease.

**D4. `attempt` counts deliveries of that message.** It starts at 1 and rises on every redelivery, whatever
the cause. It is the signal to read for "this message keeps failing".

**D5. Enqueueing the same message id twice for one key is one message** — for as long as the first is still
queued. Once acknowledged, the same id may be sent again and is a new message. This is a defence against a
producer retry, not a permanent deduplication log.

## Claims and settling

**C1. A claim is valid until its lease expires.** The response says when. Renewing (heartbeat) extends it.

**C2. An expired claim is revoked, and its settles are refused.** After revocation the messages become
available to others; a settle arriving late is reported stale and changes nothing. There is no window in
which a revoked claim can still act.

**C3. A settle may name any subset of what the claim owns.** What is not named stays owed. The key stays
held until nothing is owed.

**C4. Naming a message the claim does not own does nothing, and is not an error.** This includes messages
of other keys, ids that never existed, and ids the same claim already settled.

**C5. A settle may be retried safely.** Sending it twice has the same effect as sending it once.

**C6. A nack may ask the key to wait before anyone works it again.** The wait applies to the *key*, not to
one message. Where several nacks in a claim ask for different waits, the longest applies.

## Recovery

**R1. Recovery is bounded by the lease, not immediate.** After a consumer stops responding, its messages
become available again once the lease expires and the service notices — not at the moment it died.

**R2. A consumer that dies mid-work loses no message.** Everything it had not acknowledged is delivered
again, in order, with `attempt` incremented.

**R3. Recovery does not depend on the consumer coming back.** No consumer identity has to be re-established
for its work to be reclaimed.

---

## What is *not* guaranteed

Stated plainly, because each of these is something a reader might otherwise assume:

- **Not exactly-once.** See D1 and D3.
- **Not ordered across keys.** See O3.
- **Not ordered in effect under retry.** See O4.
- **Not serialised within a batch.** See E4.
- **Not protected against a consumer that ignores its lease.** See E2 — the service can refuse its settles,
  not stop its code.
- **No priority, and no deadline scheduling.** Messages are handed out in the order they were sent, and a
  nack's wait is the only way to delay one.
- **No fairness promise between consumers.** Waiting consumers are woken by a broadcast and race to
  claim; the instance that just finished a key is often the one that takes it next. A hot key therefore
  tends to stay on one instance. Nothing is lost — a key is worked by one consumer at a time regardless —
  but do not read "several consumers" as "the work is spread across them" for a single key.
- **No fairness promise between keys.**
- **Backlog counts are a lower bound, not a snapshot.** Producers may append to a key while it is held, so
  any count of what is queued behind may be larger by the time it is read.
- **Durability is the store's, not the service's.** If the backing store loses data, messages go with it.
  What "durable" means here is a deployment question, not a property of this contract.

## Writing tests against this

Each invariant above is meant to be assertable. Some need care:

- **Anything about exclusivity (E) needs two consumers**, and needs the second to *try* while the first
  holds. A test with one consumer proves nothing about E1.
- **E2 is only half testable.** That a lapsed consumer's settle is refused is easy to assert. That it is no
  longer *running* is not observable from outside — the assertion is on the service's side of the contract,
  and the consumer's side is a code review, not a test.
- **Anything about recovery (R) needs a real lease to expire**, so it needs a real clock, and a lease short
  enough that the test is not slow — but not so short that a slow machine expires it mid-handler and turns
  a correctness test into a flake.
- **Anything about ordering (O) needs at least three messages.** Two cannot distinguish "kept its place"
  from "went to the back".
- **O4 and E4 are the two that pass by accident.** A test that settles in order, or works one message at a
  time, will pass whether or not the service enforces anything. To test them, deliberately settle out of
  order.
- **D3 cannot be asserted positively** — a duplicate may or may not occur. What is testable is that a
  consumer *forced* to duplicate (kill after work, before settle) sees the message again with a higher
  `attempt`.
- **Prefer asserting through the API.** A test that reaches into the store asserts an implementation, and
  will fail for the wrong reason when the implementation changes. Where the store must be inspected — the
  cases where a fault is invisible from outside — say so in the test, and say what would otherwise hide it.
