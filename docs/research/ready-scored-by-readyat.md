---
title: "Scoring `ready` by when a key becomes claimable — backoffs without the watchdog"
type: research
status: superseded
updated: 2026-11-06
tags: [redis, ready, backoff, retry-after, nack, watchdog, ordering, schema, pre-research, not-a-decision]
---

# Scoring `ready` by when a key becomes claimable

> **Set aside for precision, the same day it was written.** It was measured and works for nacks, but
> nothing publishes a key becoming claimable, so the readiness would need a timed look, and it cannot
> hold back one message, only a whole key. Releasing backoffs and scheduled messages on time is now
> explored in [`tick-driven-mover.md`](tick-driven-mover.md): a mover that makes the move a write again,
> so the wake stream announces it. What stays useful here is the measurement (what a sorted set costs
> against a list, and that the claim stays a head operation), and the idea itself, should removing
> `delayed` ever be worth a schema bump on its own.

## The problem

A nack's `retry_after` is released by the watchdog, not by the clock (see
[`redis-data-structures.md`](../architecture/redis-data-structures.md)):

1. At claim end, `settle.lua` runs `ZADD delayed GT (now + retryAfter) key` and leaves the key out of
   `ready`.
2. Every `sweep-interval` (5 s by default), each instance's watchdog runs `sweep.lua` over the queues it
   knows. The script's second half moves keys whose time has passed from `delayed` back into `ready`, with a
   wake.

That has two consequences:

- **A backoff is late by up to one `sweep-interval`.** A 1 s `retry_after` is redelivered anywhere from 1 s
  to about 6 s after the nack. Several instances, each on its own 5 s cycle, shorten the average but not
  the worst case. Lowering the interval makes every instance run the sweep script more often against every
  queue it knows.
- **Only the queues an instance has served are released.** `Watchdog.queues` is filled by enqueue and
  dequeue only, and lives as long as the process. After every instance restarts, a held-back key whose
  queue nobody has touched yet stays in `delayed` until somebody does.

## The idea

Make a key's score in `ready` **the moment it becomes claimable**, and let a backoff be a score in the
future. `delayed` folds into `ready`.

Nothing then has to happen at the due time. The key doesn't move, and no sweep or promotion runs: once the
store's clock passes the score, the key is simply claimable.

An earlier version of the idea kept `delayed` and had `claim.lua` move due keys into `ready` before it
pops. That also takes the watchdog off the path, but it still moves keys, and it keeps a structure whose
only job is to say "not yet". Putting the time in the score says "not yet" without either.

## What changes

| | today | `ready` scored by readyAt |
|---|---|---|
| a claim takes | `ZPOPMIN ready` | the lowest score ≤ now: `ZRANGEBYSCORE ready -inf now LIMIT 0 1`, then `ZREM`. O(log N) |
| an empty claim reports | nothing (`nil`) | the lowest score left, which is when the next key becomes claimable, or nothing when `ready` is empty |
| a nack with `retry_after`, at claim end | `ZADD delayed`; the key stays out of `ready` | `ZADD ready (now + retryAfter) key` |
| a claim that ends with nothing to wait for | `ZADD ready <INCR seq> key` | `ZADD ready <now> key`, with "now" as in [Ties](#ties-the-counter-comes-back-as-a-clock) |
| the producer's check | key in none of `ready`, `claimed` or `delayed` | key in neither `ready` nor `claimed`. A held-back key is in `ready` with a future score, and `ZADD NX` leaves it alone, so an enqueue can't make it claimable early |
| the watchdog's reclaim | pushes to `ready` unless `ZSCORE delayed` says otherwise | pushes to `ready` with the pending backoff's time, if there is one, or now |
| the watchdog's due sweep | releases elapsed backoffs | **deleted** |
| `QueueStore.attemptClaim` | `Option[Grant]` | a grant, or "nothing claimable, the next key is due at T" |
| a parked dequeue | waits for a wake, or its patience | waits for a wake, its patience, or T, whichever comes first |

**Cross-key order keeps its meaning.** Keys are served in the order they became claimable, which is what
the counter encodes today. For a backoff, "when it became claimable" is now literally its score, so a key
whose wait has just ended goes behind keys that became claimable before it and ahead of keys that became
claimable after it.

### Waking a parked consumer at the due time

Nothing in the store happens when a backoff ends, so nothing appends a wake. The consumer side has to know
when to look:

- An empty claim returns the next due time T, and the dequeue parks for `min(patience, T − now)`. That's
  the shape of the lock's wait, where `Position.Queued(ticket, recheck)` and `Turn.Wait(delay)` name the
  time a waiter looks again.
- Every parked waiter computing its own T would make every one of them look at the same moment. The
  timer belongs in `QueueReadiness` instead: **one timer per queue per instance**, which offers a token at T.
  A token reaches exactly one consumer, so a due key costs one claim attempt per instance, the same as a
  wake does today.
- A token that turns out spurious, because another instance claimed the key first, is a wasted look. That
  is already acceptable ([`readiness-and-wake.md`](../architecture/readiness-and-wake.md)).
- **Nobody parked** on any instance: the next dequeue's first claim finds the key, so nothing is lost.
- **The released-queues gap goes away.** The key is claimable for whoever asks, on any instance, whatever
  that instance has served.

## Two things to solve

### Ties: the counter comes back as a clock

A score in milliseconds ties, and `ZRANGEBYSCORE` breaks ties by member name. That is the reason the score
is a counter today: 200 keys enqueued back to back produced 133 distinct millisecond scores, so a third of
them were served alphabetically ([`claimable-keys-as-a-set.md`](claimable-keys-as-a-set.md)).

**The candidate:** a monotonic microsecond clock per queue. "Now" for a key becoming claimable is
`max(now_µs, last + 1)`, and `last` is stored in place of `{Q}:seq`, one string per queue, as today.

- A key becoming claimable now never ties with another one, and the score stays within microseconds of
  real time.
- A backoff's score is `now_µs + retryAfter`. Two backoffs due in the same microsecond can still tie. O3
  promises no order between keys, so that's acceptable, and rare.
- A microsecond timestamp is about 2^51, which a double holds exactly (good until about the year 2255).
- **Claim deadlines (`claimed`) and fences don't change.** Only `ready`'s score changes.

To verify: that Valkey formats a Lua number of that size into `ZADD`'s argument without losing digits.
Recent Redis versions convert exactly. Plain Lua 5.1's `%.14g` would not, so this needs a test against the
version we run, not a reading of the docs.

### A backoff asked for while the claim goes on

A claim may be settled piece by piece. A partial nack can ask the key to wait while the rest of the batch
is still owned, and today that is `ZADD delayed GT` with the claim alive.

Under this design the key **can't go into `ready` while it is claimed**: once its time passed, it would be
claimable while still held, which is a double claim. So the wait has to be kept somewhere until the claim
ends:

- **`{Q}:retry`**, a hash: key → the longest wait asked for in this claim. `settle.lua` keeps the larger
  of the old and new values, which is what `GT` does today.
- **At claim end:** `settle.lua` reads it, `HDEL`s it, and uses it as the key's score in `ready`.
- **On reclaim:** the watchdog does the same for a lapsed claim, which today is the `ZSCORE delayed`
  check.
- It is a hash rather than a zset, because nothing ranges over it any more: it is only read by key, at
  the two places a claim ends.

The double-push trap in "Two rules that are easy to break" goes away with it. There is only one place a
key can be put back, so there's no second one to forget to check.

## What it doesn't cover: scheduled messages

A key's score delays **the whole key**. A scheduled message is **one message**, and the key's other
messages must stay claimable while it waits.

- It can't wait in `msgs`, since later messages would then queue behind it: option (b) in the scheduling
  note.
- It can't set its key's score either, since that would hold back messages sent before it.

So scheduled messages still need their own `{Q}:scheduled` set, from which the claim moves due entries
into `msgs`. The empty claim's "next due" becomes the earlier of `ready`'s lowest score and `scheduled`'s.
The timer described above is then the one that wakes for both, so scheduling needs no timer of its own.

## Complexity, and what a sorted set costs against a list

**The claim stays a head operation.** `ready` is sorted by readyAt, so the key that becomes claimable first
is always at the head:
- **The head is due:** it's the one to claim.
- **The head isn't due:** nothing is, because every other score is larger.

`ZRANGEBYSCORE ready -inf now LIMIT 0 1` lands on the head without walking past keys that aren't due, and
the `ZREM` of the head is the same front removal `ZPOPMIN` does: a pointer update per skiplist level, with
nothing to compare. An empty claim's "next due" is the head's score, for free.

**The dequeue as a whole**, in `claim.lua` today, with N claimable keys, C claims in flight and a batch of
b:
- O(log C + b) of real work;
- the pop at the head of `ready`: ≈ log₄ N, but pointer updates only;
- the lease written into `claimed`: the one real insertion search, O(log C);
- O(b) for the batch's ids, ownership, attempt counts and payloads;
- about 7 + b Lua-to-Redis calls.

C is bounded by concurrency, not by backlog. The readyAt version adds one call and no complexity.

**Where a sorted set does cost more than a list:** on the push, an O(log N) `ZADD` against O(1) `RPUSH`,
and in memory. Measured on 2026-11-02 with a throwaway Valkey 8.1 container on the laptop (Docker
Desktop). Each figure is 100,000 iterations of "push one member, take the head" inside one script, so the
size stays N, timed with `SLOWLOG` (server-side, no network). It is the median of three runs, minus a
baseline of two trivial calls per iteration (≈1.2 µs, the loop and the Lua bridge):

The four columns are four ways of doing the same thing, putting a key in and taking the head out:
- **list:** `RPUSH` + `LPOP`, how `ready` worked before it became a sorted set. For comparison only.
- **sorted set, counter score:** `ZADD` scored by `seq`, which always lands at the tail, + `ZPOPMIN`. What
  ships now.
- **readyAt claim:** `ZRANGEBYSCORE -inf now LIMIT 0 1` + `ZREM` of the head, then a push at the tail. The
  proposed claim.
- **backoff push:** `ZADD` scored somewhere in the middle + `ZPOPMIN`. A nack's `retry_after` under the
  proposal: the key lands at its due time, not at the tail.

| N | list | sorted set, counter score (now) | readyAt claim (proposed) | backoff push (proposed) |
|---:|---:|---:|---:|---:|
| 100 | 0.75 µs | 2.92 µs | 3.67 µs | 5.48 µs |
| 1,000 | 1.64 µs | 0.96 µs | 1.71 µs | 0.90 µs |
| 100,000 | 1.62 µs | 1.10 µs | 2.03 µs | 1.76 µs |
| 1,000,000 | 1.62 µs | 1.28 µs | 2.14 µs | 1.81 µs |

| N | list memory | zset memory | per member |
|---:|---:|---:|---|
| 100 | 1.8 KB (listpack) | 2.1 KB (listpack) | 18 B against 21 B |
| 1,000 | 17 KB (quicklist) | 88 KB (skiplist) | 17 B against 88 B |
| 1,000,000 | 17 MB | 87 MB | 17 B against 87 B |

What the numbers say:
- **Time is not the price.**
  - The switch from list to sorted set has already been made, and it cost no time: from 1,000 keys up, a
    sorted set's push and pop are as fast as a list's, or faster. The log factor is visible only as +0.3 µs
    from 1,000 to 1,000,000 keys.
  - Scoring by readyAt changes what the score means, not the structure.
  - The readyAt claim adds about 0.9 µs, which is one more call across the Lua bridge.
  - Small sets (≤ 128 members, stored as a listpack) are the slowest per operation: up to 5.5 µs for a push
    that lands in the middle, because a listpack insert moves memory. That is still microseconds.
  - All of it is two or three orders of magnitude below a dequeue's round trip, which is milliseconds under
    load ([`throughput-first-numbers.md`](throughput-first-numbers.md)).
- **Memory is the price**: about 5× per claimable key above 128 members, ~87 bytes against ~17. A million
  claimable keys in one queue cost ~87 MB of `ready` instead of ~17 MB. It scales with claimable *keys*,
  not messages, and it is paid today already, since `ready` has been a sorted set since
  [`claimable-keys-as-a-set.md`](claimable-keys-as-a-set.md).

Caveats: one laptop, synthetic members (`order-<9 digits>`), and a single script per figure. That is enough
to rank the options and see the order of magnitude, not to quote.

### End to end, without nacks

The same day, the throughput sweep from [`throughput-first-numbers.md`](throughput-first-numbers.md) was
run on a slice of this design built only for the benchmark, and thrown away afterwards:
- `enqueue.lua`, `settle.lua` and `sweep.lua` score `ready` by a monotonic microsecond clock,
  `max(now_µs, last + 1)`, kept in `{Q}:seq`, in place of `INCR seq`;
- `claim.lua` pops the head and, if its score has not passed yet, puts it back and reports nothing
  claimable.

The `retry` hash, the readiness timer and the port change were left out. The sweep acknowledges every
message, so there are no nacks and no future scores, and what it measures is the cost of the clock (`TIME`
+ `GET` + `SET` per push, instead of one `INCR`) and of the due check.

The runs were interleaved (`main`, readyAt, `main`, readyAt) so that drift on the laptop hits both
variants alike. Each run built its own image, and its log records the commit it built. Figures are the
mean of the two runs per variant, in msg/s:

| keys × consumers | drain, `main` | drain, readyAt | overall, `main` | overall, readyAt |
|---:|---:|---:|---:|---:|
| 8 × 8 | 2,183 | 1,990 (−9%) | 1,367 | 1,238 (−9%) |
| 16 × 16 | 3,561 | 3,361 (−6%) | 2,202 | 2,180 (−1%) |
| 64 × 8 | 2,352 | 2,435 (+4%) | 2,080 | 2,137 (+3%) |
| 64 × 16 | 3,874 | 3,954 (+2%) | 3,131 | 3,130 (0%) |
| 64 × 32 | 6,214 | 6,200 (0%) | 4,597 | 4,545 (−1%) |
| 128 × 64 | 9,209 | 8,774 (−5%) | 6,487 | 6,456 (0%) |
| 256 × 128 | 12,211 | 12,387 (+1%) | 8,777 | 8,843 (+1%) |

**No difference that stands out from the noise:**
- **The signs are mixed.** Every readyAt figure is within, or close to, the range of the two `main` runs.
- **Both −9% figures on 8 × 8 come from one run.** The other readyAt run (2,105 drain, 1,364 overall)
  matches `main`. That shape is known to vary by about 13% between runs.
- **`main` varies as much against itself.** 16 × 16 overall was 2,353 in one run and 2,051 in the other.
- **The largest consistent gap is at 128 × 64:** 3–7% on drain and dequeue in both readyAt runs. If it is
  real, it is the three commands per push in place of one. Two runs per variant cannot tell it from noise.

So, without nacks, scoring by readyAt costs nothing measurable end to end, which agrees with the
microbenchmark: about a microsecond inside Redis, against milliseconds per message.

One finding from the setup: **Valkey 8.1 freezes `TIME` for the whole script.** Every call to it inside one
script returns the same value. The monotonic clock above already handles it: several keys becoming
claimable in one script get `last + 1`, `last + 2`, and so on.

## What it costs

- **A schema version bump, v3 → v4.** An old instance's `ZPOPMIN` would pop held-back keys early, which is
  exactly the misread the version exists to prevent ([`redis-cluster.md`](../architecture/redis-cluster.md)).
  That means the full drain ceremony.
- **`ZCARD ready` no longer means "claimable now".** The claimable count becomes `ZCOUNT ready -inf now`,
  and `ZCARD ready` becomes claimable plus held back. That affects:
  - metrics;
  - `redis-state-walkthrough.md`;
  - `redis-data-structures.md`;
  - any spec that counts `ready`.
- **Readiness gets a timer.** It is the first time `QueueReadiness` does anything on a clock, and its
  interaction with the seed token and the hand-on needs the same care those had.
- **The sweep script loses half its body.** The watchdog's remaining job is lapsed claims. Reclaiming them
  on the claim path, as the lock reclaims an expired lease at the next acquire, is a separate question and
  not part of this.

## What it gains

- **A 1 s `retry_after` is redelivered after about 1 s.**
- **One structure fewer (`delayed`) and one sweep fewer**, and one rule fewer to break: the double push.
- **Backoffs no longer depend on which instance served what.**
- **Scheduling builds on it** without a timer of its own.

## Tests that should come first

As with the move to a sorted set, the ordering tests come before the change. Each one must pass against
the current code, or it isn't testing what it says:

- cross-key FIFO across a backoff: a key whose wait ends goes behind keys that became claimable earlier,
  and ahead of later ones;
- a nack with a short `retry_after` (say 200 ms), redelivered within a bound well under `sweep-interval`.
  This is the one that fails today and should pass afterwards;
- a partial nack during a live claim, then claim end. The key is not claimable before its wait, and is
  claimable exactly once after it;
- the same partial nack followed by a lapsed claim: the reclaim respects the wait;
- an enqueue to a held-back key doesn't make it claimable early;
- ties: a burst of keys made claimable inside one millisecond is served in arrival order. Name the keys
  against the alphabet, as the earlier FIFO test had to.

## Open questions

- **Timer precision.** Should the readiness timer fire exactly at T, or slightly after, to absorb clock
  skew between the instance and the store? The score is on the store's clock, and the timer runs on the
  instance's.
- **What an empty claim returns** when the next due key is far away: whether there should be a cap, so
  that a timer never runs past a dequeue's patience for nothing.
- **The microsecond clock.** Whether `last + 1` can drift ahead of real time. It only does when one queue
  makes more than a million keys claimable a second, which no realistic rate approaches. A score ahead of
  real time would also make that key not yet claimable, so the bound is worth stating.
