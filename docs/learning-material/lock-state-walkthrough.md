---
title: "Every lock call, and what it does to Redis"
type: learning-material
status: current
updated: 2026-09-13
tags: [lock, redis, walkthrough, keys, lua, tickets, fencing, debugging]
---

# Every lock call, and what it does to Redis

One worked example, traced through every structure, for reading a live instance or reasoning about a bug.
What each structure is *for* is [`../architecture/redis-data-structures.md`](../architecture/redis-data-structures.md);
how the mechanism fits together is [`../architecture/lock-mechanics.md`](../architecture/lock-mechanics.md);
what the API promises is [`../architecture/lock-guarantees.md`](../architecture/lock-guarantees.md). This
page is the middle: the mechanics in motion. The queue's equivalent is
[`redis-state-walkthrough.md`](redis-state-walkthrough.md).

Throughout: lock `route-7`, callers `A`, `B`, `C`. Names are shortened — every one really carries the
`{p:3}:v3:l` prefix — partition 3 of the sixteen a cluster uses, `v3` the schema version. On a single
server there is one partition and everything is in `{p:0}`. The fence counter and the
`held`/`tokens`/`waiting` structures are **shared by every lock in the partition**; only `waiters:route-7`
belongs to this lock alone. Empty structures are omitted, and `now` is whatever `TIME` said inside the
script.

## Where we start

Nothing exists. A lock that has never been taken has no keys at all — and neither has one released an hour
ago. Absence is the free state, which is why an idle lock costs nothing and why "has this lock ever
existed" is not a question the store can answer.

---

## tryAcquire, on a free lock

**`A: tryAcquire(route-7, ttl = 30s)`**

```
fence            1                      INCR — the fence rises on every grant
tokens           {route-7: 1}           A is the holder
held             {route-7: now+30000}   a future deadline: held
```

Returns the hold — `token = 1`, `leaseUntil = now+30s`. Three writes, one round trip, no waiting anywhere.

**`B: tryAcquire(route-7, ttl = 30s)`** — while A holds it:

```
(nothing changes)
```

Returns nothing. `held`'s score is still in the future, so the lock is taken and `try` never waits.

---

## acquire, when it is held

**`B: acquire(route-7, ttl = 30s, patience = 10s)`**

B cannot be granted, so it takes a ticket:

```
fence                 2                      INCR — this time for a ticket id, not a fence
waiters:route-7       [2:now+10000]          B's ticket: id 2, dead at its patience
waiting               {route-7: now+10000}   the latest ticket deadline on this list
```

Returns `Queued(ticket = 2, recheck = <what is left of A's lease>)`. B now parks on its mailbox for at most
that long.

> **The ticket id comes from the same counter as the fence, and is not one.** A ticket is drawn while
> another fence may be live, so granting with it later could hand out a number *below* one already seen
> downstream. The fence is `INCR`ed again at the moment of the grant — always.

**`C: acquire(route-7, ttl = 30s, patience = 10s)`** — arriving after B:

```
fence                 3
waiters:route-7       [2:now+10000, 3:now+10000]   C goes to the tail
waiting               {route-7: now+10000}         ZADD GT — only a later deadline moves it
```

**`D: tryAcquire(route-7, …)`, with B and C queued** — and suppose A's lease has just lapsed, so the lock
is free at this instant:

```
(nothing changes)
```

Refused, even though the lock is free. A live ticket means somebody queued first, and granting past them is
exactly the barging the tickets exist to end. **Free and queued is a real state**, and it lasts as long as it
takes the head to be woken and ask.

---

## release, and the wake

**`A: release(token = 1)`**

```
tokens           {}                     HDEL — the token can never act again
held             {}                     ZREM
{p:3}:v3:wake    +1 entry               kind=l, name=route-7
```

Returns true. Three facts about that last line:

- it is appended **in the same script** that frees the lock, so no waiter is sent to look at a lock still
  held, and no crash lands between freeing and telling;
- it is the **partition's** stream, shared with the queue — the `kind` field is what separates them;
- a **stale** release (a token that no longer matches) returns before reaching it, so no wake is sent for a
  lock that did not change hands.

**What the wake does.** Every instance's `WakeConsumer` is blocked on that stream. The entry routes through `ReadinessProcessor` to
`LockReadiness`, which wakes **every** local waiter on `route-7`. Both B and C ask; only one can win.

---

## grant, the woken crowd

**`C: grant(route-7, ticket = 3)`** — C happens to ask first:

```
(nothing changes)
```

Returns `Wait(<B's remaining patience>)`. C is not the head, so it is refused and told when the answer could
next change. C parks again.

**`B: grant(route-7, ticket = 2)`**

```
waiters:route-7  [3:now+10000]          LPOP — the head ticket is spent; C's is now the head
waiting          {route-7: now+10000}   unchanged: the list is not empty
fence            4                      INCR — the grant's fence, above every token ever issued
tokens           {route-7: 4}           B is the holder
held             {route-7: now+30000}
```

Returns the hold. C stays queued behind the new holder, and its next ask will be refused with B's lease as
the recheck.

> **Only the head can win, so the race is a check.** Waking everyone costs one refused `grant` per local
> waiter per event — and buys the property the tickets exist for: the head cannot be starved by its
> neighbours.

---

## refresh, while holding

**`B: refresh(token = 4, ttl = 30s)`**

```
held             {route-7: now+30000}   pushed forward
```

Returns the new deadline. The check is **the token, not the lease**: a holder whose lease lapsed but whom
nobody has displaced is late, not lost, and may extend. A real loss is a *token mismatch* — displacement
overwrites `tokens` — and that is refused:

**`A: refresh(token = 1, …)`** — A released long ago:

```
(nothing changes)
```

Reports not renewed. There is nothing left for token 1 to match.

---

## abandon, when patience runs out

**`C: acquire` gives up** — its 10s are spent:

```
waiters:route-7  deleted                LREM takes the last ticket, and Redis drops an empty list
waiting          {}                     ZREM follows it
```

Returns nothing to the caller. This is best-effort tidying, not correctness: a waiter that dies without
withdrawing is popped at its own deadline by the next `try`, `acquire` or `grant` to pass over the head.
The ticket is withdrawn on **every** exit that is not a grant — patience spent, caller interrupted, effect
failed — because a ticket left behind holds the queue up until its deadline.

---

## The holder that dies

No release, no refresh. B's lease simply lapses.

**A waiter's recheck fires** (or a newcomer calls `acquire`): the script reads `held`, sees a score in the
past, and treats the lock as free. The grant overwrites the stale entries:

```
fence            5
tokens           {route-7: 5}           overwritten
held             {route-7: now+30000}
```

**Nothing recovered anything.** An expired lease is not a live one, so the next grant simply wins. There is
no recovery pass on this path, and no wake either — which is why a refusal always names a recheck delay:
the death of a holder is silent, and the recheck is what makes it visible.

---

## trim, the one background pass

`LockCleanup` calls it periodically. Two sweeps, both bounded by `limit`:

**Holds expired past the grace** (`held` score older than `now - grace`):

```
tokens           entry deleted
held             entry deleted
{p:3}:v3:wake    +1 entry per freed lock     kind=l
```

The grace is a contract, not a tuning knob: since `refresh` is token-only, a holder whose lease lapsed may
still extend, so a trim — a displacement by nobody — must only take holds no live holder could plausibly
still mean to extend.

**Waiter lists whose every ticket is long dead** (`waiting` score older than `now - grace`):

```
waiters:<name>   DEL
waiting          entry removed
```

This is the one case nothing else cleans: if every waiter on a lock is gone and nobody ever asks for that
lock again, no script passes over its head to prune it. The `waiting` index exists so this sweep does not
have to scan.

---

## Reading a live instance

For lock `route-7`, first find its partition — `Math.floorMod(name.hashCode, 16)` — then, with `{p:3}:v3:l`
as `L`:

```
ZSCORE  L:held route-7             # a future millis value = held; nil or past = free
HGET    L:tokens route-7           # the live holder's fence token
LRANGE  L:waiters:route-7 0 -1     # the queue, head first, as id:deadline
ZSCORE  L:waiting route-7          # the latest ticket deadline on that list
GET     L:fence                    # the partition's counter
XREVRANGE {p:3}:v3:wake + - COUNT 5   # recent wakes, both kinds
```

Reading these is safe but racy by nature — every one of them can change between two commands, and only the
scripts see a consistent view. For a question like "is this lock held", prefer `ZSCORE L:held` over
inferring from the waiters list: a lock can be free with waiters queued, for as long as it takes the head to
be woken and ask.
