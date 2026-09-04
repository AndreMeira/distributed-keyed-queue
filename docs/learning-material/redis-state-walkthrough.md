---
title: "Every request, and what it does to Redis"
type: learning-material
status: current
updated: 2026-09-04
tags: [redis, walkthrough, state, lua, claims, debugging]
---

# Every request, and what it does to Redis

One worked example, traced through every structure, for reading a live instance or reasoning about a bug.
What each structure is *for* is [`../architecture/redis-data-structures.md`](../architecture/redis-data-structures.md);
what the service promises is [`../architecture/guarantees.md`](../architecture/guarantees.md). This page is
the middle: the mechanics in motion.

Throughout: queue `orders`, key `k1`, worker `w1`. Names are shortened — every one really carries the
`{w:0}:q:orders` prefix — bucket 0, the single-bucket default. Empty structures are omitted.

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

One call. The key leaves `ready` and the claim exists in the same script, so there is no moment in which it
belongs to nobody:

```
ready        []                       the key leaves, inside the script
msgs:k1      [m1, m2, m3]             UNCHANGED — claimed messages do not move
owned:k1     {m1, m2}                 the claim holds the first two
state        {k1: processing}
claimed      {k1: <now+lease>}        the lease
fence        {k1: 1}                  the token this claim will settle with
attempts     {m1: 1, m2: 1}           one delivery each
```

This used to be two steps — a blocking move into a per-connection holding list, then a script — and the gap
between them was the most delicate thing in the design: a key off `ready` with no lease, recoverable only by
tracking each connection's liveness. Making the claim atomic deleted the gap, the holding list, the worker
identities, and the sweep that recovered them.

**If the consumer dies now**, `claimed` holds the lease and the lapsed-claim sweep is the whole recovery
story. Nothing else can be holding a key.

**If nothing was claimable**, the script answers with nothing and the caller waits on the queue's signal,
which is raised when an entry naming that queue arrives on its bucket's `wake` stream — appended by whatever
next makes a key claimable. That wait costs a fiber, not a connection: one listener per instance reads every
bucket on one connection, from startup, so a queue nobody has asked for yet is heard as promptly as a busy
one.

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
claimed      {k1: <now+lease>}        the lease is pushed out — only if the token still matches
```

A receipt whose token no longer matches is reported back as stale rather than renewed: the claim was
revoked while the consumer was not listening. Nothing else changes — a heartbeat never moves a message.

**A consumer holding nothing has nothing to beat for.** It is known by its receipts, not by a registration,
so there is no liveness of its own to keep alive.

---

## The repair sweeps

Run by every instance, on a timer, without coordination.

**Lapsed claim** — `claimed` says the deadline passed. With the claim granted in one call, this is the
only kind of death there is:

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
