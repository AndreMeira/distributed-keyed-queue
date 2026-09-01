---
title: "A dequeue that could wait twice as long as it asked"
type: session
status: current
updated: 2026-08-31
tags: [dequeue, connections, patience, blmove, bug-fix]
---

# A dequeue that could wait twice as long as it asked

Found while writing the README's line about `DKQ_CLAIMERS`, by asking where that bound is implemented and
following it into `Connection.pool`. One real bug, two corrections to how the pool was being described, and
a design deferred.

## The bug, fixed

`Pool.provideBlocking` borrows a connection *before* `RedisQueueStore.claim` sets the `BLMOVE` timeout from
the caller's patience. So on a saturated pool the clock started twice: a caller could queue for a
connection for most of `max_wait`, then wait its full patience again. Total call duration ran past what the
caller asked for and past what the service advertises; only the client's own gRPC deadline bounded it.

A caller cannot tell whether it is parked on a Redis connection or on a ZIO queue, and should not have to —
so patience is now a **deadline**. `claim` stamps the clock before borrowing and passes only the remainder
to `BLMOVE`; a caller whose patience went entirely on the queue is answered `None`, which is what a wait
that found nothing would have given it anyway.

Regression test: `a dequeue's patience covers queueing for a connection, not only the wait for work` — the
spec's pool has one claiming connection, so two concurrent claims with 2s patience must both answer within
one patience rather than two. Verified failing before the fix and passing after.

One consequence worth knowing: under a saturated pool a `Dequeue` can now return empty while work is
sitting on the queue. That is the honest answer to "I waited as long as I said I would", and it is a
symptom of the connection bound rather than of this fix.

## Two things the pool was being described wrongly as

- **`DKQ_CLAIMERS` bounds `BLMOVE`s, not `Dequeue` calls.** A caller that cannot get a connection parks on
  `idle.take` and is served FIFO. Consumers beyond the bound are still waiting, just not on Redis — so work
  is not left unclaimed merely because consumers outnumber connections.
- **The limit bites on queue *count*, not consumer count.** `BLMOVE` waits on one key, so N queues need N
  parked connections; and because connections go to callers rather than to queues, skewed demand can starve
  a queue that has an idle consumer.

The README's `DKQ_CLAIMERS` line was written from the wrong model and has been corrected.

## Deferred, with a design written down

The fix that follows from the second point — **one watcher per queue rather than a connection per caller**,
which moves the bound from concurrent consumers to distinct queues watched — is designed but not built, in
[`../research/dequeue-connection-model.md`](../research/dequeue-connection-model.md), alongside the
notification-based model that would remove the bound entirely.

Deliberately parked: it is the core exclusivity path, the design has a real decision left in it (whether
`DKQ_CLAIMERS` should be renamed once it counts watched queues), and the current behaviour is correct if
narrow. To be picked up before the streaming API, which depends on idle consumers being cheap.
