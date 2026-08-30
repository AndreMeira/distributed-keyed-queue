---
title: "Look-ahead and discard — a design that became batch claiming"
type: research
status: superseded
updated: 2026-08-30
tags: [look-ahead, conflation, backlog, discard, batch, exploration]
---

# Look-ahead and discard

> **Superseded by batch claiming, and kept for the reasoning.** Everything below was built and then
> replaced: `backlog_depth`, `look_ahead`, `ahead`, `discard_ahead` and `discard` are all gone from the
> wire. What replaced them is a claim over N of a key's messages, settled one at a time —
> [`../architecture/redis-data-structures.md`](../architecture/redis-data-structures.md) describes what
> exists now. This note is worth keeping because the route here explains why the destination looks as it
> does.

## The problem it started from

Several messages queue behind one another on the same key, and an older one is often superseded. A consumer
had to claim, work and settle each in turn, doing the work every time. The win was never round trips —
handler time dominates — it was **skipping work**.

## Three designs, in the order they were tried

**1. Depth only.** `Delivery.backlog_depth`, plus `discard_ahead` on the settle: "there are four behind
you; drop three of them". Enough for latest-wins, because depth > 0 already means *this* message is
superseded. Cheap: one `LLEN`, one `LTRIM`.

It was too little power for anything else. A consumer that wants to decide *selectively* — "the one behind
is a delete, so skip mine" — cannot do it by counting.

**2. Depth plus the messages.** `look_ahead` on the request, `ahead` on the response. This is where the
storage format first pushed back: `msgs` held whole serialised messages, so `LRANGE` returned payloads and
Redis could not project them. Headers-only would have been *more* work than sending the messages — parse,
then strip — so full messages it was, opt-in and clamped.

**3. Ids, and then ownership.** Naming messages rather than counting them needed Redis to see an id, which
meant splitting storage: `msgs` became a list of ids and `payloads` a hash. Once ids were first-class, the
question "may this consumer discard that message?" turned out to be the same question as "does it own it?"
— and at that point the honest shape was a claim over several messages, not one message plus a view of its
neighbours.

## What survived

- **The storage split** — ids in a list, payloads in a hash. Introduced for discard-by-id, and now the thing
  that makes batch claiming possible at all.
- **`MessageId` as a type**, and `message_id` required on enqueue.
- **Idempotent enqueue**, which came free with `HSETNX` on the payload hash and honours a guarantee the
  proto had been claiming all along.
- **The clamped limit** — `look-ahead-limit` became `max-batch-limit`, same shape, same reason.

## What it taught, and would teach again

- **The storage format decides what the API can offer.** Every version of this feature was shaped by what
  Redis could see inside `msgs`, and no amount of wire design worked around it.
- **A count is safe against concurrency in a way a name is not, and a name is precise in a way a count is
  not.** Counting from the head could never catch a message that arrived after the count was read; naming
  ids needs ownership to mean something. Batch claiming supplies the ownership, which is why naming became
  safe.
- **`uint32` on the wire is a signed `Int` in Scala.** Both `discard_ahead` and `look_ahead` needed a
  non-negative check for the same reason, and a negative `LTRIM` start would have kept the tail and dropped
  everything before it. That check survives as `NegativeMaxBatch`.
