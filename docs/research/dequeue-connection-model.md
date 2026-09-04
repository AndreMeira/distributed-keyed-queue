---
title: "What a waiting consumer should cost"
type: research
status: superseded
updated: 2026-08-31
tags: [dequeue, connections, blmove, pubsub, cluster, backpressure]
---

# What a waiting consumer should cost

> **Resolved by Option B.** The notification design was built; Option A — a watcher per queue, keeping
> `BLMOVE` — was skipped. This page is kept for the framing and the measurements that led there. See
> [`non-blocking-dequeue.md`](non-blocking-dequeue.md).

Today it costs a Redis connection. This page is about what else it could cost, and what each option buys.
Nothing here is built; the current model is described first because two of its properties are easy to get
wrong from the outside.

## Where the bound actually is

`Connection.pool` opens one shared connection plus `claimers` *claiming* connections and puts the latter in
a `Queue.bounded[Claiming](claimers)`. `Pool.provideBlocking` is `acquireRelease(idle.take)(idle.offer)`,
and `RedisQueueStore.claim` is its only caller — everything else runs on the shared connection, so a burst
of parked claims cannot stall a settle or a sweep.

Two consequences that are not obvious:

- **`DKQ_CLAIMERS` bounds `BLMOVE`s, not `Dequeue` calls.** A caller that cannot get a connection is not
  refused: it parks on `idle.take`, holding nothing, and is served FIFO when a peer returns one. From the
  consumer's side, waiting on Redis and waiting on a ZIO queue are indistinguishable.
- **So work is not left unclaimed merely because there are more consumers than connections.** As long as
  one connection is parked on a queue's `ready` list, an enqueue there is picked up at once. Extra
  consumers simply queue for their turn.

The bound bites in two shapes instead:

1. **More queues than connections.** `BLMOVE` waits on one source key, so covering N queues needs N parked
   connections. Past `claimers` queues, some `ready` list provably has no watcher, and work there waits for
   a connection to rotate — up to `max_wait` — rather than for a consumer.
2. **Skewed demand under the limit.** Connections are allocated to *callers*, not to queues. Eight
   consumers on queue A take all eight; a lone consumer on B waits behind them, and B's work sits there.

## Settled while working this out

- **Patience is a deadline.** Queueing for a connection spends a caller's wait as surely as waiting on
  Redis does. `RedisQueueStore.claim` now stamps the clock before borrowing and passes only the remainder
  to `BLMOVE`; a caller whose patience went entirely on the queue is answered `None`. **Fixed, with a
  regression test** — before it, a saturated pool could make a call last twice its `max_wait`.
- **Two consumers on one instance must not both block on the same `ready` list.** A key goes to one of them
  either way, so the second `BLMOVE` spends a connection to duplicate a wait. This is the observation the
  next design turns on.

## Option A — a watcher per queue

Give the connection to the *queue* rather than to the caller.

- A **watcher** starts on demand for a queue, borrows one claiming connection, `register`s its worker, and
  loops `BLMOVE ready:{q} → claiming:<watcher>`.
- A **consumer** creates a `Promise`, enqueues it as a waiter, and awaits it with its own patience. It
  holds no connection, so patience is exact by construction.
- On a key, the watcher hands it to the first waiter whose `succeed` returns true — a consumer that timed
  out interrupts its promise and is skipped. **If no waiter takes it, the watcher releases the key back to
  `ready`.**
- The watcher exits and returns its connection when the last waiter leaves. Creation and teardown belong in
  one `Ref.Synchronized` update, or a waiter arriving during shutdown is lost.

**`consume.lua` does not need the blocking connection.** It takes the worker as an argument and `LREM`s
that worker's claiming list, so the claiming script can run on the *shared* connection, in the consumer's
own fiber. The blocking connection does nothing but wait. The consumer keeps an `onInterrupt(release)`, so
a key handed to a consumer that dies goes back at once instead of waiting for the sweep.

**What it changes:** the bound moves from *concurrent consumers* to *distinct queues being watched*. A
hundred consumers on one queue cost one connection. `DKQ_CLAIMERS` would come to mean "queues this instance
can watch at once", which is a rename worth considering (`DKQ_WATCHERS`).

**What it does not change:** N queues still need N connections. Shape (1) above survives; shape (2) does
not.

**The test that would prove it:** with `claimers = 1`, enqueue to two keys of one queue and run two
concurrent claims. Today the second waits for the first's `BLMOVE` to time out; with a watcher, one
connection serves both well inside their patience.

## Option B — notification instead of blocking

Make waiting an in-process fact entirely.

1. Try to claim **non-blocking** (`LMOVE`). On a busy queue this is the whole story.
2. If empty, park a fiber on a promise. No connection held.
3. Wake it from a notification. `produce.lua` already knows when it pushes a key onto `ready`, so it can
   `SPUBLISH` on a channel carrying the *same hash tag* — `{q:orders}:wake`. Sharded pub/sub is
   slot-scoped, which works in cluster precisely because of the tagging the key layout already uses. One
   subscriber connection per instance.
4. **Re-poll on a slow tick.** Pub/sub is fire-and-forget: a notification published between "found nothing"
   and "subscribed" is gone. Subscribe-then-check ordering plus a periodic re-check turns a missed wake-up
   into latency rather than a hang.

Connections then become one shared and one subscriber, and waiting consumers are bounded by fibers.

**Why not `BLMPOP`.** Redis 7 can block on several keys at once, which would let one connection watch many
queues — but every key in a multi-key command must share a slot, and queues deliberately carry different
hash tags. It would work standalone and break in cluster, so it is not a step worth taking.

## The constraint both options must keep

**Demand equals workers: every claimed key gets a worker or is given back.** The wake-up may be
centralised, but the *claim* must still happen in the fiber that will do the work — `consume.lua` inside
the request, as it is today. A dispatcher that claims ahead of demand into a buffer recreates work held for
a consumer that has since gone, which is the failure the toolkit's `PollConsumer` spent a day on
(https://github.com/AndreMeira/homelab-toolkit-zio). The watcher in Option A only moves a key with `BLMOVE`
while a waiter exists, and releases it if that waiter is gone by the time it lands.

## Where this sits

Option A is the cheap step and pays immediately; Option B removes the limit and is a prerequisite for the
streaming API in [`roadmap.md`](roadmap.md) §2, where idle streams should cost fibers rather than
connections. Neither is started. The deadline fix above is the only part that has landed.
