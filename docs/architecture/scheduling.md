---
title: "Scheduling — a message that becomes claimable after a delay, and what moves it"
type: architecture
status: current
updated: 2026-11-14
tags: [schedule, delayed-delivery, timer, mover, redis, lua, lateness]
---

# Scheduling

`Schedule(queue, message, deliver_after)` accepts a message that becomes claimable after a delay. Until it falls
due no consumer sees it; from then on it is an ordinary message of its key, with every guarantee in
[`guarantees.md`](guarantees.md) (S1–S5 there). This page is how it works today. Why it works this way, and
what was weighed and dropped, is in [`../research/message-scheduling.md`](../research/message-scheduling.md) and
[`../research/tick-driven-mover.md`](../research/tick-driven-mover.md).

## The rules

- **The delay is measured on the store's clock.** The script adds `deliver_after` to `TIME`, so no caller's or
  instance's clock is compared with anything. Required and greater than zero (an unset `Duration` reads as zero
  and is refused: "deliver now" is `Enqueue`); at most `max-schedule-ahead` (7 days by default), refused rather
  than clamped beyond it.
- **The first schedule of an id wins.** A schedule for an id already scheduled for the key is ignored and
  answered `scheduled = false` with the earlier one's remaining time. There is no rescheduling and no cancel: to
  move a deadline, schedule a new message with its own id.
- **An id is compared with the key's queued messages only when the message is moved**, with enqueue's rule (D5):
  an id still queued for the key means the scheduled copy is the same message, and is discarded.
- **A moved message takes its place in its key's line then**, behind whatever was sent for the key in between.
  Messages of one key due in the same millisecond join in message-id order.

## Where a scheduled message waits

Per partition, not per queue, so a pass of the mover is one script per partition whatever the number of queues
([`redis-data-structures.md`](redis-data-structures.md)):

| key | type | maps |
|---|---|---|
| `{W}:scheduled` | zset | member → when it falls due, unix millis on the store's clock |
| `{W}:scheduled:payloads` | hash | member → the message, until it is moved |

A **member** is the message's identity: `<byte length of queue>:<queue><byte length of key>:<key><message id>`,
for example `6:orders3:c42nudge`. Keys and ids may hold any character, so no separator is safe; the
lengths are UTF-8 byte lengths, which is what Lua's `#` counts. Only Lua writes or reads one: `schedule.lua`
builds it from the queue, key and id it receives, and `move.lua` splits it.

## The two scripts

- **`schedule.lua`** — `ZADD NX` the member at `now + delay`; when it was added, `HSET` the payload and answer
  `{1, delay}`; otherwise answer `{0, remaining}` from the existing score.
- **`move.lua`**, one partition at a time — take up to `schedule-limit` members due by `TIME`; for each, remove it
  and apply enqueue's rules to its key: `HSETNX` the key's `payloads` (a duplicate id is discarded), `RPUSH` its
  `msgs`, and `ZADD ready` with the queue's own `seq` when the key is accounted for nowhere else (`ready`,
  `claimed`, `delayed`). Then append **one wake per queue** in which a key became claimable, not one per message,
  so a burst of due messages cannot trim unread entries from the stream. Answer
  `{moved, discarded, full, millis until the next entry, or -1}`.

Both build every queue key at runtime from the partition's prefix, which is legal because the partition's tag
puts them all in one slot.

## What moves them, and when

Each instance runs a **`DueTimer`** and a **`ScheduleMover`** (`domain/service/maintenance/`), started by the
maintenance module next to the watchdog.

- **`DueTimer`** holds a `homelab.common.flow.TimeLatch` (toolkit 0.0.7), armed for the backstop at the latest.
  When it fires, the timer swaps in a fresh latch and emits a tick into a `Queue.sliding(1)`, so ticks coalesce
  and none is lost. A request ("fire no later than X") brings the latch earlier, but never closer than
  `schedule-floor`: firings are at least one floor apart, and entries due within one floor of a pass share the
  next one. The first latch fires at once, so a pass
  runs on boot.
- **`ScheduleMover`**, on every tick, calls `QueueStore.moveDue`, which runs `move.lua` on every partition. Every
  partition is tried even when another fails, but the answer is all or nothing: one unreachable partition fails
  the call. A full call is repeated at once; otherwise the mover asks the timer for a tick at the next due time
  the store reported. A failed call is recorded by its measurement, and the backstop retries.
- **The schedule use case** asks the timer for a tick at the due time it recorded, after the write. A request is
  only a hint: the store is the truth, and an instance that dies with its requests in memory loses nothing but
  time.

## Lateness

A scheduled message joins its key's line **within one floor plus the slack of its due time** (about 100 ms at
most) while the instance that accepted it runs, and **within one backstop (60 s)** if that instance dies, since any
other instance's backstop then moves it.

**Usually on time.** A tick is never asked for closer than the floor (50 ms), so an entry due more than one floor
after the last request or pass that armed the timer is moved at its due time; one due sooner waits for the floor,
up to 50 ms late. A pass for another entry re-arms the timer, so a message scheduled well ahead can still land
within a floor of that pass, and wait for it. The slack drops a request that would move the timer earlier by less than the slack,
so an entry due just before one already asked for is moved with it, up to the slack late.

The floor also spaces the ticks: every request on a latch is made after the previous firing, so firings are at
least one floor apart, however dense the due times, and entries due close together share a pass. It bounds the
ticks, not the calls to the store: a pass that comes back full is repeated at once, without a tick, so a backlog
is drained as fast as its passes run. Every instance that
learns a due time sweeps then (herding); the move is idempotent, so the extra passes find nothing.

## Configuration

| key | default | meaning |
|---|---|---|
| `max-schedule-ahead` | 7 days | the horizon: the longest delay accepted |
| `schedule-floor` | 50 ms | the shortest delay a tick is asked for, so ticks are at least this far apart |
| `schedule-slack` | 50 ms | how much earlier a request must be to move the timer |
| `schedule-backstop` | 60 s | the longest the timer waits with no request |
| `schedule-limit` | 100 | the most entries one call moves per partition |

Each with a `DKQ_*` override (`DKQ_MAX_SCHEDULE_AHEAD`, …). The e2e stacks set the backstop to 5 s.

## Backoffs: the same machinery for a nack's wait

A nack with a backoff holds back its **key** (C6). The held-back keys wait in the partition's `{W}:delayed`
(schema `v4`), members `<byte length of queue>:<queue><key>`, and the same machinery as for schedules releases
them on time:

- **`SettleUseCase`** asks the backoffs timer for a tick at the wait's end, less the store call's round trip, in
  one uninterruptible step with the settle.
- **`BackoffMover`**, on every tick, calls `QueueStore.releaseDue`, which runs **`backoff.lua`** on every partition
  concurrently: each elapsed backoff is removed, and its key goes back into `ready` unless a claim still holds it
  (the claim's end puts it back then). One wake per queue; the answer carries the next end.
- **`DueTimers(schedules, backoffs)`**: two timers, one per mover, bundled so the two can sit in the environment.
- **The watchdog** only reclaims lapsed claims now.

| key | default | meaning |
|---|---|---|
| `max-retry-after` | 1 hour | the longest backoff a nack may ask for; a longer one is clamped to it |
| `backoff-floor` | 5 ms | the shortest delay a backoff tick is asked for |
| `backoff-slack` | 5 ms | how much earlier a request must be to move the timer |
| `backoff-backstop` | 5 s | the longest the timer waits with no request |
| `backoff-limit` | 100 | the most backoffs one call releases per partition |

A backoff ends within about 10 ms while the instance that took the nack runs, within the backstop if it dies. Its
key is then claimable at once, unless a claim still holds it (a partial nack leaves the claim alive while it owes
other messages): the key then becomes claimable when that claim is settled or reclaimed, never while it lives (C6).

## Observability

Each call the movers make to the store is measured, as `ScheduleMover.move` and `BackoffMover.release` (count,
latency, failures), and `RedisQueueStore.schedule`, `moveDue` and `releaseDue` are traced. Nothing
finer is recorded, not even what a pass moved and discarded: per-message counts or a lateness histogram would take a metrics system of their own
([`observability.md`](observability.md)).

## Memory and durability

A scheduled message waits in the store's memory until it is due, and nothing drains it earlier. On a
`noeviction` store, full memory refuses every write, which is what the horizon bounds. A scheduled message is as
durable as the store's persistence: with persistence off, a restart of the store loses it (S4).

## Not built

Cancelling a schedule.

**One partition unreachable puts every partition on the backstop.** The partitions are moved or released
concurrently, and every one is tried, so each pass still handles what is due on the reachable partitions. But a
call that any partition fails is a failed call: the mover learns neither the next due time nor whether a partition
was full, so it neither re-arms nor repeats, and waits for its backstop. While a partition is unreachable the mover
therefore runs once per backstop (60 s for schedules, 5 s for backoffs, plus the command timeout, `max-wait` 30 s,
when the partition times out rather than refusing), and each pass handles at most one batch (`schedule-limit` or
`backoff-limit`) per reachable partition: a backlog drains one batch per backstop, and the unreachable
partition's own entries wait for it to come back. It only arises on a cluster with a node down (a single server has one partition). Reporting the
reachable partitions' next due time despite the failure, or a mover loop per partition, would remove it; neither
is worth its cost for one DKQ with its own Valkey per service, the scope these choices assume. That scope, and
what an infrastructure-level DKQ would rework, are in [`../research/tick-driven-mover.md`](../research/tick-driven-mover.md).
