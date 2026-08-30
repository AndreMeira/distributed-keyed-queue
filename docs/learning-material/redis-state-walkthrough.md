---
title: "Every request, and what it does to Redis"
type: learning-material
status: current
updated: 2026-08-30
tags: [redis, walkthrough, state, lua, claims, debugging]
---

# Every request, and what it does to Redis

One worked example, traced through every structure, for reading a live instance or reasoning about a bug.
What each structure is *for* is [`../architecture/redis-data-structures.md`](../architecture/redis-data-structures.md);
what the service promises is [`../architecture/guarantees.md`](../architecture/guarantees.md). This page is
the middle: the mechanics in motion.

Throughout: queue `orders`, key `k1`, worker `w1`. Names are shortened — every one really carries the
`{q:orders}` prefix. Empty structures are omitted.

## Where we start

Nothing exists. A queue that has never been used has no keys at all; absence is the empty state everywhere,
which is why an idle key costs nothing.

---

## Enqueue

Three messages for one key, sent in order.

**`Enqueue(orders, k1, m1)`**

```
payloads:k1  {m1: …}          HSETNX — the id is new, so the message is stored
msgs:k1      [m1]             appended
state        {k1: queued}     the key was idle, so it becomes claimable
ready        [k1]
```

**`Enqueue(orders, k1, m2)`, `Enqueue(orders, k1, m3)`**

```
payloads:k1  {m1: …, m2: …, m3: …}
msgs:k1      [m1, m2, m3]
state        {k1: queued}     unchanged — already queued, so `ready` is NOT pushed again
ready        [k1]             still one entry: the key is claimable once, not once per message
```

> **The state guard is the whole point.** A key on `ready` twice is two claimers for one key. Every path
> that pushes to `ready` first checks that the key is not already queued or held.

**`Enqueue(orders, k1, m2)` again** — a producer retry:

```
payloads:k1  unchanged        HSETNX finds m2 already there and does nothing
msgs:k1      unchanged        so the append is skipped too
```

Nothing changes, and the caller is told the depth. Deduplication lasts only while the message is queued;
once acknowledged, the id is free again.

---

## Dequeue

Two steps, and the gap between them is the interesting part.

**Step 1 — take the key** (a blocking move, not a script):

```
ready        []                the key leaves
claiming:w1  [k1]              …and lands here
```

The key now belongs to nobody: it is off `ready` but no claim exists. Two things depend on this list:

- **If `w1` dies right here**, only `claiming:w1` records that `k1` was taken. No lease exists, so the
  lapsed-claim sweep cannot see it; recovery is the dead-worker sweep, which is why a worker registers its
  liveness *before* its first blocking move. A worker that claimed without registering would strand the key
  for ever.
- **If `w1` merely stalls** long enough to be declared dead, the sweep gives the key back and another
  consumer may claim it. When `w1` finally wakes, step 2 finds the key gone from `claiming:w1` and
  **refuses to claim** — returning nothing, as if the wait had timed out. Without that check `w1` would
  claim messages another consumer is already working: its settles would be refused, but both would have run
  the handler.

**Step 2 — turn it into a claim**, provided the key is still in `claiming:w1`. `Dequeue(orders, max_batch=2)`:

```
claiming:w1  []                       the key is accounted for
msgs:k1      [m1, m2, m3]             UNCHANGED — claimed messages do not move
owned:k1     {m1, m2}                 the claim holds the first two
state        {k1: processing}
claimed      {k1: <now+lease>}        the lease
fence        {k1: 1}                  the token this claim will settle with
attempts     {m1: 1, m2: 1}           one delivery each
```

The response carries the receipt (the token), both messages, the lease deadline, and `backlog_depth: 1` —
m3, still queued behind the batch.

**A second consumer asking for `orders` now** blocks: `ready` is empty. It is not told the key is busy;
there is simply nothing to take.

---

## Settle

**`Settle(receipt, [m1: ack])`** — one of the two:

```
owned:k1     {m2}                     m1 is no longer owed
msgs:k1      [m2, m3]                 m1 leaves the order
payloads:k1  {m2: …, m3: …}           and its message
attempts     {m2: 1}                  and its count
```

`owned:k1` is not empty, so **the claim is still live**: the fence does not move, the lease stays, the key
is still nobody else's. The same receipt settles the rest.

**`Settle(receipt, [m2: nack])`** — the other one fails:

```
owned:k1     {}                       nothing is owed any more
msgs:k1      [m2, m3]                 UNCHANGED — a nack puts nothing back, because nothing moved
attempts     {m2: 1}                  kept, so the next delivery counts 2
fence        {k1: 2}                  the claim is over; the old token is now worthless
claimed      {}                       lease released
state        {k1: queued}
ready        [k1]                     claimable again
```

The next claim hands out `m2` first — it never lost its place — with `attempt: 2`.

**With a backoff.** Had the nack asked the key to wait:

```
delayed      {k1: <now+wait>}
ready        []                       NOT pushed: the key waits
state        {k1: queued}
```

The key sits queued-but-not-ready until a sweep finds the wait elapsed.

**Naming something the claim does not own** — another key's message, or one already settled — changes
nothing and is not an error. **Repeating a settle** is the same: the second finds nothing owed.

**Settling with a spent token** — after the claim ended, or after a reclaim — matches nothing in `fence`
and is refused. Nothing changes.

---

## Heartbeat

**`Heartbeat([receipt])`**

```
workers      {w1: <now+lease>}        this consumer is alive
claimed      {k1: <now+lease>}        the lease is pushed out — only if the token still matches
```

A receipt whose token no longer matches is reported back as stale rather than renewed: the claim was
revoked while the consumer was not listening. Nothing else changes — a heartbeat never moves a message.

**Registration and renewal are the same call.** A consumer with nothing held still beats, because a claiming
connection that stops announcing itself cannot be recovered.

---

## The repair sweeps

Run by every instance, on a timer, without coordination.

**Lapsed claim** — `claimed` says the deadline passed:

```
owned:k1     {}                       ownership forgotten
fence        {k1: +1}                 the silent consumer's token is now worthless
claimed      {}                       lease dropped
state        {k1: queued}
ready        [k1]                     …unless `delayed` says the key is still waiting
msgs:k1      unchanged                nothing to restore: nothing had moved
payloads:k1  unchanged
attempts     unchanged                a reclaim IS a delivery that did not finish, and it shows
```

**Dead worker** — `workers` says a consumer went silent, and its `claiming` list is not empty:

```
claiming:w1  []                       drained back
ready        [k1, …]                  in the order they were taken
workers      {}                       forgotten
```

This is the only thing that recovers a key lost between the blocking move and the claim.

**Elapsed backoff** — `delayed` says the wait is over:

```
delayed      {}
ready        [k1]
```

---

## Reading a live instance

What each structure tells you when something looks wrong:

| symptom | look at |
|---|---|
| a key seems stuck | `state` (is it `processing`?), `claimed` (is there a lease, and is it in the past?) |
| a consumer's settles are refused | `fence` versus the token in its receipt — something revoked its claim |
| work is not being handed out | `ready` (empty?), `delayed` (waiting?), `state` (still `processing`?) |
| a message keeps coming back | `attempts` for its id — and whether anything ever acknowledges it |
| a key is worked twice at once | `ready` for duplicate entries. It should never hold a key twice |
| memory grows | `payloads:<key>` against `msgs:<key>` — they should hold the same ids |

That last row is the one invariant no request depends on but every leak would show in: **every id in
`payloads:<key>` is either in `msgs:<key>` or being settled right now.** Anything else is an orphan.
