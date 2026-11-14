---
title: "A tick-driven mover — releasing backoffs and scheduled messages on time"
type: research
status: current
updated: 2026-11-14
tags: [design, schedule, backoff, retry-after, nack, watchdog, ticker, latch, processor, redis, exploration, not-a-decision]
---

# A tick-driven mover

> **Implemented (2026-11-14)**: the schedules pair as `DueTimer` and `ScheduleMover`, then the backoffs pair as a
> second `DueTimer` and `BackoffMover`, per partition, with the schema bumped to `v4`. The current state is
> [`../architecture/scheduling.md`](../architecture/scheduling.md). This note stays as the record of why.

> **Originally an exploration, not a decision** (its decisions are now marked "Decided" where they were
> taken). This note works out one idea in detail so it can be read and challenged as a whole. Names in
> *italics* (*the tick source*, *the mover*) are roles; the classes are `DueTimer` and `ScheduleMover` (see "The
> names" under "Decided"). The discussion that led here is in
> [`../sessions/2026-11-02-next-features.md`](../sessions/2026-11-02-next-features.md).

## The problem

Two features need something to happen **at a time**, not on a request:

- **A nack's backoff ending.** A nack with `retry_after` holds its key back. Today the watchdog releases it
  on a fixed 5 s sweep, so a 1 s backoff is redelivered anywhere from 1 s to about 6 s after the nack. The
  sweep also covers only the queues its own instance has served since booting
  ([`redis-data-structures.md`](../architecture/redis-data-structures.md), "The watchdog's two sweeps").
- **A scheduled message falling due** ([`message-scheduling.md`](message-scheduling.md)). It waits outside
  its key's line, and has to be moved into it at its due time.

In both cases the moment it becomes due is not a write: no script runs then, so nothing announces it.
Something has to wake up at that time, make the write, and let the write announce itself through the wake
stream the way every other change to `ready` already does.

## The idea in one paragraph

Each instance runs two small processors per kind of due work. *The tick source* owns a one-shot timer, a
**time latch**, armed at the latest for a backstop, and accepts requests to fire earlier. When the latch
fires, the tick source replaces it with a fresh one and emits a **tick**. *The mover* consumes ticks: for
each partition it runs a script that moves everything due, appends the wakes, and answers when the next
thing is due. The mover sends that time back to the tick source as one more request. Requests also come from
the code that creates due work: a settle that asks for a backoff, a schedule. The store is the source of
truth. A request is only a hint about when to look.

## Decided (2026-11-14), for the scheduling feature

- **Scope: the schedules pair only.** The backoffs pair needs `{p}:delayed`, a change to an existing
  structure, so a schema bump (question 7 of [`message-scheduling.md`](message-scheduling.md)). Nacks keep
  the watchdog until a feature of their own converts them; the mover's code is written once, so that pair
  reuses it.
- **Later the same day: the backoffs pair, per partition (variant B).** `{p}:v4:delayed` with members
  `<len(queue)>:<queue><key>`, so the schema goes to `v4`: DKQ was not deployed anywhere, so the bump's drain cost
  nothing. `BackoffMover` and a second `DueTimer` (bundled as `DueTimers`), `backoff.lua`, `QueueStore.releaseDue`;
  `SettleUseCase` asks for the tick; the watchdog only reclaims. Backoff quantum and slack 5 ms, backstop 5 s,
  limit 100. The variant without a bump (per queue, over the known queues) was set aside: it kept the watchdog's
  blind spot for queues nobody had touched since a restart.
- **The latch:** `homelab.common.flow.TimeLatch`, from toolkit 0.0.7.
- **The mover reaches the tick source with a direct `triggerAt`.** No request pipe, so no feedback cycle.
- **Batching: requests are rounded up to a 50 ms quantum, and `slack` is 50 ms.** This caps the mover at 20
  firings a second per instance, whatever the schedule rate, at the cost of up to 50 ms of lateness.
- **Herding is accepted.** Every instance that learns a due time sweeps then; N − 1 find nothing.
- **One wake per queue touched, for the schedules mover only.** Enqueue keeps its wake per key.
- **Limit 100 entries per script** (today's `sweep-limit`), a full pass repeated at once; **backstop 60 s.**

What that gives the lateness guarantee (S3): a scheduled message joins its line within one quantum (about
50 ms) of its due time while the instance that accepted it runs, and within one backstop (60 s) if it dies.

These choices rest on a scope assumption, stated in "Scope, and what an infra-level DKQ would rework" below.

**The names (decided 2026-11-14).** In the rest of this note, *the tick source* is `DueTimer` and *the
mover* is `ScheduleMover`, both in `domain/service/maintenance/` next to `Watchdog`.

| Layer | Name |
|---|---|
| Proto | rpc `Schedule`: `ScheduleRequest { queue, message, deliver_after }`, `ScheduleResponse { scheduled, due_in }` |
| Domain request, response | `ScheduleRequest` (`domain/request/queue/`), `ScheduleResponse` (`domain/response/queue/`) |
| Validated input | `ScheduledSubmission(submission: Submission, delay: Duration)` (`domain/model/queue/`); not `Schedule`, which clashes with `zio.Schedule` |
| Use case | `QueueScheduleUseCase` |
| Store port, `QueueStore` | `schedule(scheduled): IO[AdapterError, QueueStore.Scheduled]`, `Scheduled(scheduled: Boolean, dueIn: Duration)`; `moveDue(partition, limit): IO[AdapterError, QueueStore.Moved]`, `Moved(moved: Int, discarded: Int, full: Boolean, nextDueIn: Option[Duration])` |
| Scripts | `lua/queue/schedule.lua` (`ScheduleScript`), `lua/queue/move.lua` (`MoveScript`) |
| Configuration (`queue.conf`, `DKQ_*` overrides) | `max-schedule-ahead` 7 days, `schedule-quantum` 50 ms, `schedule-slack` 50 ms, `schedule-backstop` 60 s, `schedule-limit` 100 |
| Metrics | `ScheduleMover.move`: each call to the store, counted and timed by `Monitor.measure` (see "Metrics" below) |

The member of `{p}:scheduled` is built in Lua, by `schedule.lua` from the queue, key and id it receives,
and split by `move.lua`, so Scala never handles it and Lua's `#` gives the byte lengths the encoding needs.

## Components

### The time latch

A one-shot timer: **`homelab.common.flow.TimeLatch`, in `homelab-common` since toolkit 0.0.7** (toolkit PR
#74, promoted from the incubator draft `time/v3`). DKQ depends on 0.0.7 from this branch on. Its spec,
`TimeLatchSpec`, lives in the toolkit.

| | |
|---|---|
| created | `make(in, slack)`: armed to fire `in` from now at the latest. Never unarmed. `in` and `slack` are clamped to `[0, Duration.Infinity]` |
| `triggerAt(t)` / `triggerIn(d)` | brings the firing time earlier. A request later than the current time, or earlier by less than `slack`, changes nothing |
| `await` | returns once the latch has fired. It sleeps until the current firing time and sleeps again whenever a trigger brings it earlier. Every waiter is woken |
| after firing | `await` returns at once, and no trigger can make it wait again. **It is thrown away, not reset**. "Fired" is a time (`when ≤ now`), not a flag |

The properties it keeps, each fixed during a review and pinned by a test:

- **A trigger landing while a waiter starts to wait is not missed.** The firing instant and the promise that
  wakes its waiters are one value, read and replaced together.
- **A trigger is uninterruptible.** Replacing the state and waking the replaced state's waiters are one step,
  so a caller interrupted in between cannot leave them asleep through the earlier instant. That matters here:
  the schedule use case triggers on a request fiber, which a cancelled call interrupts.
- **No input makes it throw.** Delays and `slack` are clamped, and a trigger compares
  `Duration.fromInterval(instant, when)` with `slack` instead of shifting an instant. On ZIO 2.1.23 a wait of
  `Duration.Infinity` (about 292 years) sleeps, but one of a billion years makes the live timer return at
  once, and past `Instant.MAX` `Instant.plus` throws (both checked with a probe).

### The tick source

Owns `Ref[TimeLatch]`, and does two things at once, forever:

1. **Takes requests.** A request is "fire no later than `t`", where `t` is an `Instant` on the instance's
   clock. Handling one is `ref.get.flatMap(_.triggerAt(t))`.
2. **Fires.** It loops:
   - `await` the current latch;
   - make a fresh latch armed with the backstop;
   - put it in the `Ref`;
   - emit a tick.

It is not a plain `Processor`, because it runs two loops. Its `run` races or zips them, which `Processor`'s
doc allows ("overrides `run` only if the standard loop does not suit").

### The mover

Consumes ticks. For one tick, it:

1. runs the move script once per partition (16 on a cluster, 1 on a single server);
2. collects each script's answer: how many entries it moved, whether it hit its limit, and when the next
   entry is due, as "in X ms" on the store's clock;
3. sends the tick source a request:
   - `now` if any partition was full, so the next pass comes at once;
   - otherwise `now + min(X)` over the partitions that still have something waiting;
   - nothing when none has.

A failed script is logged and skipped, as `Watchdog.sweep` does today. The mover must not stop on the first
failure, which `Processor.serial` would do. The next latch's backstop fires regardless.

### Two pairs

There is one tick source and mover pair per kind of due work, each with its own configuration:

| pair | moves | into | backstop | why separate |
|---|---|---|---|---|
| backoffs | held-back keys from `{p}:delayed` | `ready` | seconds | short waits want a short backstop |
| schedules | messages from `{p}:scheduled` | their key's `msgs` (and `ready` if the key is idle) | a minute or more | long waits, and large batches due together |

A burst of schedules due together therefore cannot hold up backoff releases, and each pair has its own
limits, logs and metrics. The code is written once and instantiated twice.

## Data

Both sets are **per partition**, tagged like everything else in the partition, so a move script reaches every
queue's keys in that partition within one cluster slot:

| key | type | maps |
|---|---|---|
| `{p}:delayed` | zset | `<queue>` + `<key>` → when the key may be worked again, unix millis (store clock) |
| `{p}:scheduled` | zset | `<queue>` + `<key>` + `<message id>` → when the message is due |
| `{p}:scheduled:payloads` | hash | the same member → the message |

The member's encoding must not collide with characters a queue name, key or id may contain. For
`{p}:scheduled` it is decided: length-prefixed, id last (`6:orders3:c42nudge`), see question 2 of
[`message-scheduling.md`](message-scheduling.md). `{p}:delayed` would reuse the same form without the id.

**Why per partition, not per queue.** Per queue would be correct too: a move only matters to someone who
dequeues the queue, and a dequeue registers the queue on its instance. Per partition was preferred because a
tick's work is bounded and small (one script per partition, whatever the number of queues) and because it
needs no registration and no in-memory list of queues
([`message-scheduling.md`](message-scheduling.md) records both strategies).

**What changes for nacks:**
- `delayed` moves from `{Q}:delayed` to `{p}:delayed`;
- `settle.lua` writes it there;
- `enqueue.lua`'s check and the watchdog's reclaim read it there;
- the watchdog's due sweep is deleted.

This is a schema bump to `v4`, with the drain ceremony in [`redis-cluster.md`](../architecture/redis-cluster.md).

## The flows

### A nack with a backoff

1. `settle.lua` holds the key back: `ZADD {p}:delayed GT <now + retryAfter> <queue+key>`.
2. The settle use case sends this instance's backoff tick source a request for `now + retryAfter`.
3. The latch fires at that time, the tick source swaps the latch and emits a tick, and the mover runs the
   backoff script on every partition.
4. The script removes the elapsed delay and, **only if the key is not in `claimed`**, moves it into `ready`
   and appends a wake for its queue. Consumers on every instance learn of it through the stream, exactly as
   they do today.
   - **Why the check matters:** a partial nack can set the delay while the rest of the batch is still owned
     (`settle.lua` returns with the claim alive). Publishing that key would let `claim.lua` grant it again
     while the original claim is valid, which breaks E1.
   - **A key still claimed is left out of `ready`.** With its delay removed, the claim's end or a reclaim
     publishes it, since both push when `delayed` holds nothing for the key.
   - **Today's watchdog has the same gap:** `sweep.lua`'s due sweep pushes without checking `claimed`. It
     is being fixed on its own, ahead of any of this.
5. The script answers with the next due time, which arms the new latch.

### A scheduled message

1. The schedule use case writes the message into `{p}:scheduled` and its payload into
   `{p}:scheduled:payloads`, in one script. The script answers with how long until the message is due,
   "in X ms" on the store's clock.
2. It sends this instance's schedule tick source a request for `now_local + X`. It never sends the due
   time itself, which is an instant on the store's clock, while the latch sleeps on the instance's (see
   point 4 below).
3. On the tick, the schedule script moves each due message into its key's line:
   - `HSETNX` the key's `payloads`;
   - `RPUSH` its `msgs`;
   - `ZADD ready` if the key is idle, which means in none of `ready`, `claimed` and `{p}:delayed`;
   - one wake per queue touched.
4. The script answers with the next due time.

### Boot

A new instance knows nothing about what is due. Its first latch should be armed to fire **at once**
(`make(0, …)`), so the first sweep runs immediately and its answer arms the next latch from the store. That
is what makes a deploy or a restart cost one sweep rather than one backstop period.

### An instance dies with requests in memory

Its early ticks are lost. Every other instance's backstop sweeps every partition, so the due work is moved at
the latest one backstop period late. Nothing is lost: the work is in the store, and only the hint was in
memory.

## What makes it correct

The invariant everything rests on: **the store is the truth; a request is a hint about when to look, and is
made only after the write it is about.** From that:

1. **A request landing on a fired latch is not lost.** Between the latch firing and the tick source
   swapping in a fresh one, a request hits the fired latch and is refused. That request was made after its
   write to the store, and the tick about to be emitted comes before the move. So the move sees the entry:
   it moves it if it is due, and otherwise reports its due time. Either way it is covered.
2. **A request after the swap lands on the new latch.** So the order inside the tick source is: fire, swap,
   emit. Nothing needs a lock.
3. **Ticks coalesce, and a tick is never lost.** The pipe from the tick source to the mover is
   `Queue.sliding(1)`. When the mover is still busy and a second tick is emitted, the buffer holds one: "sweep
   again". Two pending ticks mean the same thing as one. The latch is an edge, the pipe makes it a level, and
   that is the level-triggered behaviour the mover needs (a tick missed mid-sweep could be the only record of
   a nack the sweep did not see).
4. **The answer is relative, so the clocks never meet.** Due times are on the store's clock (`TIME`), and
   the latch sleeps on the instance's clock. The script answers "in X ms", and the mover arms
   `now_local + X`, so skew between the two clocks shifts nothing.
5. **Moves are idempotent across instances.** Every instance's mover may run the same script at the same
   moment. One moves, and the others find nothing due. The script is atomic, so there is no double move.

## Waking consumers: one wake per queue

Every key made claimable today appends its own wake, and the partition's stream is trimmed at about 1,000
entries. A move that releases hundreds of entries in one script would append that many wakes, and could
trim away entries a briefly lagging listener has not read: the one gap R4 says nothing detects.

The readiness is per queue, and a wake's `key` field is only for humans, so the move script can append **one
wake per queue it touched**. A batch of 1,000 messages across 3 queues then costs 3 entries. *(To evaluate:
whether the same should hold for the backoff mover, and whether enqueue's per-key wake should follow.)*

## Batching

A tick moves everything due in a partition, up to the limit, in one script. What decides how much a tick
batches is how close together the firing times are.

- **`slack` on the latch** drops a trigger that would move the firing time earlier by less than `slack`, so
  a due entry may be up to `slack` late. It coalesces triggers. It does not gather entries due slightly
  after the firing time.
- **Quantising the requested time** (*to evaluate*) gathers them. Round every request up to the next
  multiple of a quantum, say 50 ms, so entries due within the same window share one firing and one sweep,
  each at most one quantum late.
- **Moving early is not an option.** A message would be claimable before its due time, which breaks S1 in
  the scheduling note.

## Herding

Every instance whose mover sweeps learns the next due time from the store and arms its latch with it. So at
that time, every instance sweeps, and N − 1 of them find nothing. That is harmless, because the move is
idempotent, but it costs 16 × N scripts per due instant. The options, *to evaluate*:

- **Accept it.** The cost is small, and every instance has a precise latch, so the loss of one costs
  nothing.
- **Only the instance that made the request arms early.** The others keep only the backstop, and feed
  their sweep's answer into nothing. Then one instance sweeps per due instant, and a crash costs one
  backstop of lateness.
- **Jitter** the herd's firing by a few milliseconds, so the first sweep usually wins before the others
  start.

## Pipes and the feedback loop

The tick source feeds the mover, and the mover feeds requests back to the tick source. With bounded,
blocking pipes, that cycle can deadlock: each side blocks offering to the other.
- **The tick pipe is `sliding(1)`.** It never blocks, which already breaks the cycle.
- **Requests cannot be dropped,** since an earlier one matters. Two shapes:
  - **The mover calls the tick source's `triggerAt` directly.** There is no pipe and no cycle, and it is the
    simplest.
  - **An unbounded request pipe.** It is symmetric, and the volume is small: one request per nack or
    schedule, plus one per sweep.

The use cases that create due work (settle, schedule) send their requests the same way the mover does.

## Configuration, per pair

| setting | backoffs | schedules | meaning |
|---|---|---|---|
| backstop | ~5 s (today's `sweep-interval`) | ~60 s | the longest the latch waits with no request; bounds lateness when early ticks are lost |
| slack | a few ms | ~50 ms | the least a trigger must bring the firing time forward to count |
| quantum | none, or a few ms | ~50 ms (*to evaluate*) | the window entries due together share |
| limit | today's `sweep-limit` (100) | larger? | the most entries one script moves; a full pass repeats at once |

## What stays as it is

- **The readiness, `DequeueUseCase`, and the wake stream.** A move is a write that appends its own wake.
- **The watchdog's reclaim of lapsed claims.** It keeps its fixed interval, since a lease expiring has
  nobody to request a tick for it. Only its due sweep goes.
- **The claim path.** `claim.lua` is untouched.

## Tests it would need

**The time latch:** done, in the toolkit's `TimeLatchSpec` (11 cases on the `TestClock`, one on the live
clock), including the interrupted trigger and the extremes of `Instant`.

**The tick source:**
- fire, swap and emit happen in that order;
- a request between the fire and the swap is covered by the tick;
- ticks emitted during a slow move coalesce into one.

**The move scripts**, against Redis:
- due entries move and later ones stay;
- one wake per queue;
- a full pass reports it;
- the answer is the next due time in ms;
- a scheduled message for an idle key makes it claimable;
- one for a claimed key waits behind the claim.

**End to end:**
- a nack with a 200 ms `retry_after` is redelivered well under the old 5 s bound;
- a scheduled message is claimable within a bound of its due time;
- killing the instance that took the request still delivers it within the backstop.

## Alternatives considered

| alternative | why not, for now |
|---|---|
| the watchdog's fixed sweep, with a shorter interval | precision costs every instance a sweep of every known queue at that rate, and only known queues are swept |
| `ready` scored by readyAt ([`ready-scored-by-readyat.md`](ready-scored-by-readyat.md)) | measured cheap, but nothing publishes a key becoming claimable, so the readiness would need a timed look; and it cannot hold a single message back, only a whole key |
| promotion on the claim | a parked consumer does not claim at the due time, so it still needs a timer |
| a reusable ticker (`time/v1`, `time/v2`, incubator drafts kept on the toolkit's `incubator-time-drafts` branch) | v1 is an edge-triggered broadcast, and a tick missed mid-sweep can be the only record of a nack; v2 works, but a throwaway latch plus a sliding pipe gets the same behaviour with less state |

## Metrics

**Decided (2026-11-14): `Monitor.measure` only.** Each call the mover makes to the store is measured as
`ScheduleMover.move`: how many passes run, how long each takes, and how many fail. What a pass moved and
discarded is in its log line, not a metric.

The finer metrics first proposed (`moved`, `discarded`, `lateness` as move time − due time, `backstop` for moves
made by a backstop tick) are not recorded: `Monitor` counts and times operations, and stays that simple on
purpose. A deployment that wants per-message numbers or a lateness histogram is asking for a metrics system of
its own (StatsD or similar), not for a richer `Monitor`.

## Scope, and what an infra-level DKQ would rework

**The assumption:** one DKQ, with a Valkey of its own, per service (the recommended deployment, see
`research/infrastructure/valkey-per-service.md` in the workspace). A service has a handful of queues and a
few instances. Within that scope the choices above hold. If DKQ became an infrastructure service shared by
many tenants, like SQS, four places would need reworking:

1. **The wake stream** (shared with enqueue, which already appends one wake per key). One stream per
   partition, trimmed at a fixed length, read by every instance: the reading cost grows as instances ×
   wakes, and a listener lagging past the trim misses wakes undetected (R4). One wake per queue helps only
   while a batch's messages fall into few queues; with thousands of queues they fall into as many queues as
   messages, and it saves nothing. The rework is in the wake design (per-queue or bucketed streams, lag
   detection), not in the mover. [`bucketed-wake-streams.md`](bucketed-wake-streams.md) explored part of it.
2. **Herding.** 16 × N scripts per due instant grows with the number of instances: fine for a few per
   service, not for a fleet.
3. **The shared per-partition limit.** One queue's burst holds another's due work back by a pass. Within a
   service that is one owner's queues delaying each other; across tenants it is a noisy neighbour.
4. **Memory isolation.** Under `noeviction`, schedules that fill the memory make *every* write fail. Within
   a service the horizon (`max-schedule-ahead`) is enough; across tenants it would take per-tenant quotas.

## Open questions

- **For the backoffs pair, when it comes:** its quantum, whether it also wakes once per queue, and whether
  enqueue's wake should follow. (The schedules pair's are decided above: direct `triggerAt`, herding
  accepted, a 50 ms quantum, one wake per queue.)
- **The member encoding** for `{p}:delayed`. Decided for `{p}:scheduled` (length-prefixed, id last).
- **Metrics: decided (2026-11-14), see "Metrics" above.**
