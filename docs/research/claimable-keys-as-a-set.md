---
title: "Making claimable keys addressable"
type: research
status: draft
updated: 2026-09-04
tags: [redis, ready, state, ordering, stickiness, design]
---

# Making claimable keys addressable

Not built. Two things that turn out to be the same change: removing the `state` hash, and being able to
claim a *named* key rather than whatever is at the head.

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
