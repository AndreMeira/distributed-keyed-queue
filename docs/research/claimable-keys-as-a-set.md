---
title: "Making claimable keys addressable"
type: research
status: current
updated: 2026-09-05
tags: [redis, ready, state, ordering, stickiness, design]
---

# Making claimable keys addressable

Built, except for the last step. Two things that turn out to be the same change: removing the `state` hash,
and being able to claim a *named* key rather than whatever is at the head. What shipped, and where it
departed from this note, is at the end.

## What `state` actually does

`{q:Q}:state` maps key → `queued` | `processing`, with absence meaning idle. It is written in four places
and **read in one** — `produce.lua`, and only for existence:

```lua
if not redis.call('HGET', state, key) then
  redis.call('HSET', state, key, 'queued')
  redis.call('RPUSH', ready, key)
  redis.call('XADD', wake, …)
end
```

Nothing ever branches on `queued` versus `processing`. The labels are for a human reading `HGETALL`.

So its one job is to answer, for a producer: **"is this key already accounted for?"** A key is accounted
for when it is on `ready`, *or* claimed by somebody, *or* waiting out a backoff in `delayed`. In the last
two it is deliberately absent from `ready`, and the producer still must not push it — because a key that
appears twice in `ready` is claimed twice, concurrently, which is the one thing the design forbids.

`ready` cannot answer that question today: it is a list, so membership is O(N), and it does not know about
the two states in which a key is accounted for but not in it.

## The change

Make `ready` a **sorted set**, scored by when the key became claimable.

| today | proposed |
|---|---|
| `HGET state` guard, then `RPUSH ready` | `ZADD ready NX <score> <key>` — uniqueness is the structure's |
| `LPOP ready` | `ZPOPMIN ready` — still oldest-first across keys |
| `state` says claimed / backing off | ask `claimed` and `delayed`, which already know |
| claim a named key: `LREM`, O(N) | `ZREM ready <key>`, O(log N) |

`state` then has no reader and is deleted. The producer's guard becomes three membership checks —
`ready`, `claimed`, `delayed` — instead of one.

## The four score decisions

The scores *are* the cross-key ordering, so they have to agree. Each push site answers "when did this key
become claimable":

1. **`produce.lua`, idle key** — `now`. It has just become claimable.
2. **`produce.lua`, key already on `ready`** — `ZADD NX` leaves the existing score alone. A key that has
   been waiting must not lose its place because another message arrived for it.
3. **`complete.lua`, claim ended with messages left** — `now`. It has been served; it goes behind whatever
   has been waiting.
4. **watchdog, lapsed claim or elapsed backoff** — `now`, for the same reason.

With a list, `RPUSH` being the only way in makes all four agree by construction. With scores they are four
decisions that can drift apart, and drift is invisible: nothing currently asserts cross-key FIFO. **A test
for it should come before the change**, not after — the e2e ordering test asserts order *within* a key.

## Why it is worth doing anyway

**It removes a structure whose value is never read**, and with it a small trap: `state` looks like it
records what a key is doing, so a reader reasonably assumes something depends on `processing`. Nothing does.

**It makes a named claim cheap**, which is what any affinity or stickiness scheme needs. The wake entry
already carries the key; with a zset an instance that recently worked `k` can claim `k` by name while its
peers take the head, so the two claims are for different keys and do not contend at all. With a list that
is an O(N) `LREM` over every claimable key, which rules it out at any interesting size. See the stickiness
discussion in [`dequeue-connection-model.md`](dequeue-connection-model.md).

## Why not in the wake-stream PR

Independent: the wake-stream design works identically over a list or a zset, so bundling them produces one
change that reworks both how a claim is *taken* and how claimable keys are *stored* — with no way to
bisect between them later.

## Order of work

1. A test that asserts cross-key FIFO — several keys enqueued in a known order, one consumer, assert the
   keys are served in that order. It should pass against the list first, or it is not testing what it says.
2. `ready` to a zset, `state` deleted, the four scores as above.
3. Only then the named claim, and whatever affinity policy is worth having on top of it.

## What shipped, and the one place it deviates

Built in the order the note asks for.

**The test came first, and it needed sharpening twice.** A cross-key FIFO test written against the *list*
passed, as it must to be worth anything. Then it passed against the sorted set too — and was still wrong:
its keys were named `k1…k5`, so a tie in the scores would have been broken by member name in exactly the
order the test expected. Renaming them `k5…k1`, against the alphabet, is what makes a tie fail.

**The score is a counter, not a timestamp.** This is the deviation. The note says each site scores a key
with "when it became claimable", which is the natural reading and is wrong at any interesting rate:
`ZPOPMIN` breaks a tie by member name, so keys becoming claimable within the same millisecond would be
served *alphabetically*. That is not hypothetical — 200 keys enqueued back to back produced **133 distinct
millisecond scores**, so a third of them were tied. `{Q}:seq` is `INCR`ed once per key that becomes
claimable, which cannot tie and makes the four sites agree by construction rather than by discipline. It
replaces `state` one-for-one in the key count.

**The producer's guard is three membership checks**, as described: `ready`, `claimed`, `delayed`, all
inside the one script that adds the key, so they are atomic together.

**`state` is deleted.** Idle is now the absence of a key from all three structures.

Throughput is unchanged — two sweeps either side of the change land within run-to-run spread, with the
8-key shape marginally *better* (1,333–1,348 against 1,143). Nothing here was expected to move it; the
change buys a structure removed and a named claim made cheap.

Not built: the named claim itself, and any affinity policy on top of it. `ZREM ready <key>` is now O(log N),
so the door is open.

## What step 3 would pull, when it happens

Deferred deliberately — the set change stands on its own, and the named claim is a feature with its own
design questions rather than a piece of this one. What is already known about it:

- **Two flavours, with different requirements.** *Consumer-driven* — "I hold warm state for `k`, give me
  `k`" — needs only a request field and a claim script that does `ZREM ready <key>` instead of `ZPOPMIN`.
  *Announcement-driven* — "`k` just became claimable and I worked it last" — needs the key to reach the
  consumer, and it currently does not: a wake entry carries `queue` and `key`, but `Waiters.raise(queue)`
  discards the key, because a signal is per queue and every waiter on that queue receives the same one.
  Routing a key to a particular waiter would mean a signal per key, or a side channel.
- **The fallback is a fairness decision**, not a detail: when the named key is not claimable, answering
  empty, falling back to the head, and waiting for that key specifically are three different contracts.
- **The payoff is less contention, not locality alone.** An instance asking for `k` by name while its peers
  take the head is claiming a *different* key from them, so the two do not race.
- [`dequeue-connection-model.md`](dequeue-connection-model.md) discusses stickiness, but predates both the
  signal and the sorted set; its framing needs a pass before it is much use.
- [`dequeue-latency.md`](dequeue-latency.md) draws the distinction that matters before starting: a named
  claim reduces *contention* while keeping the authority in the store, so it still pays a round trip per
  claim. Leasing a slice of the keyspace is what removes the round trip, and it is a much larger change.
