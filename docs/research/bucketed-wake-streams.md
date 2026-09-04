---
title: "Bucketed hash tags: a wake stream per bucket, not per queue"
type: research
status: current
updated: 2026-09-04
tags: [redis, cluster, hash-tags, wake, streams, latency, design]
---

# Bucketed hash tags: a wake stream per bucket, not per queue

Built on `broadcast-bell-one-stream`, and measured below. One change to where the hash tag comes from,
which makes the listener's subscription set fixed instead of growing — and with it takes `DKQ_WAKE_BLOCK`
off the latency path entirely.

## The problem

`WakeListener` issues one `XREAD` across every queue it has been asked to watch. The stream set is fixed
when the read is issued, so a queue watched while a read is in flight is not heard until that read returns —
either an entry lands on another watched stream, or `block` expires. An instance watching nothing sleeps for
`block` instead, and a `watch` arriving during the sleep does not shorten it.

Nothing is lost: `watch` resolves a concrete stream id before the consumer's first claim, `XREAD` is
non-destructive, and the position only advances on a successful read. It is latency, bounded by `block`, and
paid once per queue per instance — `watched` is only ever added to.

Measured on the e2e deployment with `DKQ_WAKE_BLOCK: 200 millis`, first message on a queue no instance had
ever watched, against a control where the queue had been watched for a second:

```
cold queue, message sent 50ms after parking   median 122ms, p95 207ms, max 207ms
warm queue, message sent 1s after parking     median  12ms, p95  17ms, max  17ms
```

Spread uniformly across the block, as the mechanism predicts. At the 1 second default that is roughly 500ms
typical and 1s worst, for the first message on a new queue.

The objection this note exists to answer is not really the number. It is that an interval sits on a latency
path at all — that a design which advertises push delivery still has a poll-shaped fallback in it.

## Why not just move the stream, keeping the tags

Making the stream global while queues stay tagged `{q:orders}` puts it in a different slot from
the keys whose claimability it announces, and
[`../architecture/redis-cluster.md`](../architecture/redis-cluster.md) already says what that costs:

> **The `wake` stream is per queue for the same reason.** A global stream would be a different slot, so
> the entry could not be appended by the script that made the key claimable — and a separate append is a
> crash window where work exists and nobody is told.

The fix is not to move the stream. It is to move **the tag**, so that the stream and the keys it announces
stay in one slot by construction.

## The proposal

Tag by **bucket** rather than by queue, and give each bucket one wake stream.

```
{q:orders}:ready        →   {w:7}:q:orders:ready
{q:orders}:wake         →   {w:7}:wake            ← shared by every queue in bucket 7
```

`bucket(queue) = hash(queue) % buckets`, fixed for the deployment. A bucket's stream is in the same slot as
the queues that append to it, so the append stays inside the script that made the key claimable — the atomicity
the per-queue layout was protecting is kept, and the listener's stream set is known at startup and never
grows.

### The bucket count is the only knob

The same code is the one-stream design and the sharded design, at different `buckets`:

- **`buckets = 1`** — one tag, one stream, one slot. The listener issues a single `XREAD`, never re-issued
  because a queue appeared. Atomicity intact, because the single tag puts the keys in that slot too. This is
  the one-stream design, and on a single node it gives up nothing.
- **`buckets = 16` or `64`** — sharded wake streams, sharded queues, atomicity still intact, and cluster
  mode actually spreads.

Sharding the stream costs no plumbing: one `XREAD` names many streams — which is how the per-queue listener
already works — so reading 64 bucket streams is one command on one connection.

### What changes

- **`Namespace`** — the only place key names are built. The prefix gains the bucket tag and keeps the queue
  as a plain segment, so keys stay readable and queues stay isolated.
- **The Lua does not change.** Every script already takes `prefix` as an argument and `wake` as a declared
  key; both simply arrive with different contents.
- **`WakeListener`** — `watch(queue)` disappears entirely, along with `position` and the growing `watched`
  map: the streams are known at startup, so every one of them is in every read from the first.
- **The Lua changes after all**, in one small way. An entry carried `key` — the *message* key — and the
  queue came from the stream name. A shared stream cannot say, so `produce.lua`, `complete.lua` and
  `watchdog.lua` take the queue as an argument and write `'queue', queue, 'key', key`. Three lines, three
  argument builders, and the listener routes on that field.
- **Configuration** — one new value, the bucket count.

## What `block` becomes

Not a latency bound. For a stream already in the read, delivery is push: `XREAD` returns the instant an
entry lands, whatever `block` is set to. Once the set is fixed, nothing waits on the interval, and the
`if watched.isEmpty then sleep(block)` branch — the one a cold consumer can land in the middle of —
disappears, because there is always a stream to read.

What is left are three bounds nobody is blocked on:

- **Liveness.** A silently half-open connection never returns from a blocked read; re-issuing is what
  discovers it. Note the coupling: the listening connection's Lettuce command ceiling is `maxWait + 10s`
  slack precisely so it exceeds the longest block. Raise one and the other has to follow.
- **Shutdown.** The server-side block outlives the fiber, so a stopping instance waits out the read.
- **Topology.** In cluster mode, re-issuing re-resolves where a stream lives after a failover or resharding.

So `block` can go up — 3 seconds, or more — without a message waiting a millisecond longer. Two conditions:

1. **It is only safe once the set is fixed.** On today's layout, raising `block` raises the cold-start
   penalty in direct proportion.
2. **Split the retry backoff from the block.** `run` currently reuses `block` as the pause after a failed
   read, which *is* on the latency path: at 3 seconds, a blip leaves the instance deaf for 3 seconds. A long
   block and a short, escalating retry are different knobs that happen to share a name today.

The extreme — `BLOCK 0`, never return — is viable if the command timeout is disabled and liveness is left to
TCP keepalive and Lettuce's connection watchdog, at the cost of a harder shutdown. Worth knowing; not worth
doing first.

## Wakes missed while disconnected

One wake can still be missed, and bucketing makes it slightly more likely, since the trim budget is shared
by every queue in the bucket (the arithmetic is in *Reading the stream* below). A listener reading
continuously never notices. A listener that is *disconnected* long enough comes back to a position that has
been trimmed away — and `XREAD` does not report that, it simply hands over what is left. The wakes in the
gap are gone, and with a signal there is nothing behind them, so the consumers parked on those
queues wait out their patience.

The repair is on the error path, where the listener already knows something went wrong:

```scala
read.flatMap(wake).catchAll(_ => raiseAll *> ZIO.sleep(retryBackoff)).forever
```

Raise every local signal before retrying. It costs one claim attempt per parked consumer, once, after a failure
that already means trouble — and it covers every wake missed during the outage, not only the trimmed ones.

This is deliberately **not** the silent-turn backstop that the signal design removed. That one fired on a
quiet, healthy read — an interval on the happy path. This fires on a detectable event and never runs while
things work.

A precise version exists if it is ever wanted: `XINFO STREAM` reports the first entry still held, and
`first-entry-id > lastId` means entries were trimmed unread. One command per reconnect would say whether the
raise is needed. Raising everything is cheaper than the check and wrong in a harmless direction.

## Reading the stream: how much, and how long it survives

Two numbers govern whether a wake is ever missed, and neither is the one that looks alarming.

**`COUNT` is not the constraint.** A read returns as soon as one entry exists, so `COUNT` is a cap on a
single reply, not a batch the listener waits to fill. It drains roughly `COUNT / round trip` — at the
current 64 and a millisecond round trip, some 64,000 entries a second against a measured peak of ~4,000
messages (call it ~8,000 wakes counting requeues). In steady state the listener sits about four entries
behind a buffer of a thousand.

The 64 is an unexamined default from the first implementation. Its only defence belonged to the registry
design, where entries and waiters were one to one, so a batch bounded a burst of local wakes. Under the
signal that is gone: **a thousand entries for one queue are one wake**, so a batch should be deduplicated by
queue before raising.

```scala
val woken = Chunk.fromIterable(delivered.flatMap(...)).distinct  // one wake per queue, not per entry
```

Deduplication is correct only because of the broadcast change — collapsing entries under the old handover
would have destroyed the cardinality it depended on. Position tracking is unaffected: `lastId` still
advances to the last entry seen per stream.

So `COUNT 1000`: a thousand single-field entries is ~100KB in one transient reply, parsing is microseconds,
and after deduplication the raising is proportional to *distinct queues in the batch*. The payoff is
catch-up — after a pause with 1,600 entries waiting, 25 round trips at 64 against 2 at 1000, and those two
reads produce one wake per queue instead of 1,600 promise swaps.

**`MAXLEN` is the constraint, and it is denominated in the wrong unit.** What matters is how long the
listener may be absent before entries are trimmed unread:

```
survivable absence = MAXLEN / wake rate = 1000 / 8000 ≈ 125ms at peak
```

The same thousand entries are an hour of buffer on an idle queue and an eighth of a second on a busy one.
Trimming by age says what is meant instead, and is rate-independent — stream ids are millisecond
timestamps, so `XADD ... MINID ~ <now - 30s>` keeps thirty seconds of wakes whatever the load. The buffer
should hold for as long as a listener may plausibly be away, which is a duration, not a count.

**The absence raise-on-reconnect does not cover** is a silent one: a long GC pause or a descheduled
container produces no error, so nothing fires, and entries can be trimmed unnoticed. That is where the
precise check earns its keep — `XINFO STREAM` reports the oldest entry still held, and `first-entry-id >
lastId` means entries were trimmed unread. It can be gated cheaply on a read coming back *full*, which is
the signal that the listener might be behind.

## What it costs

- **The bucket count is permanent**, like a partition count. Changing it moves queues between tags and
  strands their keys under the old one, so it is fixed at first use and any change means drain and restart.
  Cheap for this POC, which has no persistence; the reason this is a branch of its own rather than a
  refactor.
- **Queues sharing a bucket share a slot.** "Sharding spreads queues" weakens to "sharding spreads
  buckets", and one hot queue can make its whole bucket's node hot.
- **Fan-out within a bucket.** An instance serving one queue in bucket 7 hears the wakes of every queue in
  bucket 7, and discards what it does not serve.

### And specifically against `buckets = 1`

- **The whole store lives on one node.** One tag is one slot, and a slot has one master; the rest of a
  cluster idles. This does not merely fail to use cluster mode, it forecloses it — and resharding cannot
  help, because there is nothing to spread.
- **Every instance reads the whole system's wakes.** With M instances that is M× the firehose, where
  bucketing gives each instance only the buckets it touches.
- **One key takes every write** — every enqueue, every requeuing settle, every watchdog release.

The trimming risk is not on this list: raise-on-reconnect covers it at any bucket count.

None of these bite on a single Valkey, which is what this POC runs. Hence the recommendation: build the
bucket function, default it to 1, and leave the count a config value — the code is identical at any N, and
cluster mode later becomes a value and a drain rather than a redesign.

## What it measured

Built on `broadcast-bell-one-stream` at `buckets = 1`, against the same e2e deployment and the same probe
that produced the numbers above:

| | per-queue streams | one bucket |
|---|---|---|
| **cold queue** — first message on a queue no instance had watched | median **122ms**, p95 **207ms** | median **16ms**, p95 51ms |
| warm queue — control, watched for a second first | median 12ms, p95 17ms | median 13ms, p95 16ms |

The cold path collapsed onto the warm one, which is the whole claim: there is nothing to start watching, so
a queue's first consumer waits for a claim attempt rather than for a read to come round. Throughput was
unchanged (1165/1846/2197/3288/3902 msg/s across the sweep, against 1043/1765/1791/3087/4192 before — inside
run-to-run noise), and the idle-consumer wake latency stayed at 9ms.

Two things the implementation corrected in this note:

- **The Lua did change**, as recorded above — the entry had to start naming its queue.
- **`block` is now genuinely off the latency path**, and the retry backoff was split from it at 200ms, so a
  failed read costs a fifth of a second of deafness rather than a whole block.

Not measured: cluster mode, and any bucket count above one.

## Open questions

- **Read every bucket, or only the ones served?** The implementation reads every bucket, which makes the set
  constant for the process's life and removes the cold path entirely; reading only the buckets served would
  keep fan-out down at the cost of a set that changes up to `buckets` times. Moot at `buckets = 1`, and the
  question reopens the day a deployment runs more.
- **How high can `block` go**, and what should the retry backoff be once they are separate knobs?
- **Does the queue segment stay in the key name?** Not needed for correctness once the tag is the bucket,
  but dropping it makes a live Redis unreadable by queue.
- **How long should the buffer hold?** Trimming by age turns this into one number — thirty seconds of
  wakes, or five minutes — chosen from how long a listener may plausibly be absent.

## What to measure

- The cold-start probe in `ThroughputSpec`, which produced the numbers above — it should collapse to the
  warm numbers.
- The throughput sweep, for the fan-out cost of hearing a bucket's other queues.
- A cluster-mode run, since none of the failure modes here are visible on a single node.

## Relationship to other notes

- [`non-blocking-dequeue.md`](non-blocking-dequeue.md) — where the per-queue wake stream came from.
- [`claimable-keys-as-a-set.md`](claimable-keys-as-a-set.md) — independent; it changes what `ready` is, not
  where the tag comes from.
- The signal is also independent: it decides who hears a wake inside an instance, not which stream
  carried it. It is what makes raise-on-reconnect necessary, having removed the backstop that used to cover
  the same gap by polling.
