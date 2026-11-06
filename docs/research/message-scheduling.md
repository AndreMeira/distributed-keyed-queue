---
title: "Message scheduling — a message that becomes claimable at a time (pre-research)"
type: research
status: draft
updated: 2026-11-02
tags: [api, schedule, delayed-delivery, redis, watchdog, pre-research, not-a-decision]
---

# Message scheduling

> **Pre-research.** This note states the goal, what dkq already has that bears on it, the design questions,
> and the research rounds that would answer them. It decides nothing. Where it leans, it says so and why.

## The goal

A new verb on `KeyedQueue`: send a message now that **becomes claimable at a later time**. Until then no
consumer sees it. From then on it is an ordinary message of its key, with every guarantee in
[`guarantees.md`](../architecture/guarantees.md).

Its first consumer is already written down in
[`two-verbs-an-agent-consumer-asked-for.md`](two-verbs-an-agent-consumer-asked-for.md): an agent loop needs a
backstop nudge at a deadline ("if nothing else has woken this conversation by then, wake it"). Without the
verb, that is a timer beside the queue with its own durability story. With it, the nudge is an ordinary
message on the ordinary path. Delayed delivery is also something every comparable broker has, so the verb
adds little to what dkq is.

Sized as medium: one new structure, one new script, one more sweep, one new RPC, and the client.

## What dkq already has

- **A key-level delay.** A nack may set `retry_after`. The key goes into `{Q}:delayed` (key → when it may
  be worked again), and the watchdog's second sweep moves elapsed keys back into `ready` with a fresh score
  and a wake ([`redis-data-structures.md`](../architecture/redis-data-structures.md)). Scheduling is the
  per-message version of the same thing, on the enqueue side.
- **The store's clock.** Scripts read `TIME` from Redis, so "now" is one clock for every instance. A
  scheduled time compared inside a script is compared against that clock.
- **A sweep that is idempotent and runs on every instance.** No leader. `sweep-interval` is 5 s and
  `sweep-limit` bounds each script.
- **Two limits of that sweep, which a scheduled message would inherit.**
  - **Lateness.** A backoff today is released up to one `sweep-interval` late.
  - **The queues swept.** The watchdog sweeps only the queues *its own instance* has served since booting
    (`Watchdog.queues`). After every instance restarts, a queue nobody has enqueued to or dequeued from is
    not swept by anyone. For a backoff this rarely matters, because a consumer polling the queue makes it
    known. For a message scheduled hours ahead, it is the case to think about.

## The design questions

### 1. Where does a scheduled message take its place in its key's order?

This one decides the rest.

- **(a) When it becomes due**, as if it were enqueued at that moment. Messages sent for the key in between
  go ahead of it. O1 ("handed out in the order they were enqueued") holds unchanged, if "enqueued" means
  "became due". This is what the backstop nudge wants, and it adds nothing to the claim path.
- **(b) When it was sent, but not claimable until due.** Either it blocks every later message of its key
  until then, which is a key-level delay the caller did not ask for, or the claim skips past it. Skipping
  breaks the claim's "first N ids of `msgs`" and the rule that claimed messages never move.

**Leaning (a).** It keeps `msgs` meaning "the key's line" and keeps the claim untouched.

### 2. Where does it wait?

The obvious shape, given (a), is a per-queue sorted set outside the key's line:

| key | type | maps |
|---|---|---|
| `{Q}:scheduled` | zset | an entry naming key and message id → due time, unix millis |
| `{Q}:scheduled:payloads` | hash | that entry → the message |

A due sweep then runs the enqueue logic for each due entry in one script: `HSETNX payloads`, `RPUSH msgs`,
and `ZADD ready` plus a wake when the key is idle. Both keys carry the queue's tag, so this is one slot and
legal Lua. This is the shape Sidekiq's scheduled set and BullMQ's delayed set use (from memory; to
verify in round 1).

Whether the payload sits in its own hash or in `payloads:<key>` from the start depends on question 4.

### 3. How late may a due message be, and who moves it?

- **The watchdog's tick.** Lateness of up to `sweep-interval` (5 s), the same as a backoff today. It is
  the cheapest option, and it may be enough for the agent's backstop.
- **A timer for the earliest due time.** An instance reads `ZRANGE scheduled 0 0 WITHSCORES` and sleeps
  until then. Lateness becomes milliseconds plus the round trip. Every instance doing it is harmless, since
  the move is atomic and idempotent. The same timer could release `delayed` backoffs too, which would make
  `retry_after` precise as a side effect.
- **On the claim.** `claim.lua` promotes due entries before it pops. This alone is not enough: a parked
  consumer only claims after a wake, and nothing wakes it at the due time.

And who sweeps a queue after a full restart (the second limit above)? The options:
- a per-partition index of queues that have anything scheduled, which every instance sweeps;
- the existing per-instance set, accepting the gap and documenting it.

**Leaning, since this was written:** neither the watchdog nor a timer of its own.
[`ready-scored-by-readyat.md`](ready-scored-by-readyat.md) makes a backoff a future score in `ready`, so an
empty claim reports when the next key becomes claimable, and `QueueReadiness` keeps one timer per queue
that offers a token then. Scheduling would join that:
- the claim moves due entries from `{Q}:scheduled` into `msgs`;
- "next due" becomes the earlier of `ready`'s lowest score and `scheduled`'s.

Lateness is then a round trip, and since promotion happens on the claim, the restart question goes away
too. That change would come first.

**Superseded, same day: a tick-driven mover over a sorted set** (session log
`2026-11-02-next-features.md`). How it works:
- **Persisted at once.** A scheduled message is written straight into a sorted set scored by its due time.
- **The mover.** On each instance, a ticker drives it. A tick, or the periodic backstop, runs a script that
  moves due messages into their key's `msgs`, appends their wakes, and returns when the next one is due.
  The ticker is then armed with that.
- **The ticker is a shortcut, not the mechanism.** The instance that receives a schedule asks for an early
  tick at its due time. If that instance is gone, another instance's tick or backstop moves the message.

**Where the set lives is open, between two strategies.** Neither has a correctness gap: a moved message
only matters to someone who dequeues its queue, and a dequeue registers the queue on the instance that
serves it.

- **Per queue** (`{Q}:scheduled`):
  - **Sweeps:** a tick runs one script per known queue.
  - **What the registration must do:** a queue seen for the first time is swept at once, or its next due
    time is fetched to arm a tick. Otherwise the first dequeue after a restart waits for the backstop.
  - **After a full restart,** an idle queue's messages are moved when its first consumer comes back. Nobody
    was waiting for them before that.
- **Per partition** (`{p}:scheduled`, members naming queue, key and message id):
  - **Sweeps:** a tick runs one script per partition (16, or 1 on a single server), whatever the number of
    queues.
  - **No registration is needed:** every instance knows the partitions from `KeyLayout`.
  - **Due messages are moved by any backstop,** even with nobody consuming.
  - **The script stays legal Lua:** the partition's tag puts the set and every queue's keys in that
    partition in one slot, so `msgs:<key>` is built at runtime, as `claim.lua` does.

Per partition wins when there are many queues. Per queue matches today's layout.

**Preferred so far, still under evaluation: per partition.** A tick's work is bounded and small, one script per partition whatever the number
of queues, and it is simpler: no registration step, and no list of known queues to keep in step. It costs
two things:
- **The sweep limit is shared by the partition's queues,** so a burst on one queue can hold another's due
  work back by a pass. A full pass repeats at once, as the watchdog's does.
- **For nacks, a schema bump.** Held-back keys move from `{Q}:delayed` to `{p}:delayed`, members naming
  queue and key, and `enqueue.lua`'s check reads that member.

**Either way, still to handle:**
- **clock skew:** the script returns "due in X ms" on the store's clock, not an absolute instant;
- **herding:** every instance that learns the next due time sweeps then;
- **the place in the key's order,** taken at move time;
- **large batches due together.**

**To evaluate, not decided: batching through the slack.**
- **How it works:**
  - With a slack of, say, 50 ms, the requests arriving within that window collapse into one tick.
  - Per partition, that tick's script moves everything due across the partition's queues in one call, up to
    the sweep limit.
  - One round trip and one script dispatch then serve the whole batch.
- **What it costs:**
  - A due message may wait up to the slack for its batch.
  - Inside Redis, each moved entry still costs its own work: removing it from the set, the `RPUSH` into its
    key's `msgs`, and the `ZADD` into `ready` if the key was idle.
  - The batch saves the per-call overhead, not the per-message work.
  - The sweep limit still bounds how long one script holds the server.
- **The wakes are the thing to design for.**
  - Today every key made claimable appends its own wake, and the partition's stream is trimmed at about
    1,000 entries.
  - A move of hundreds or thousands of entries in one script could trim away entries a briefly lagging
    listener has not read, the one case R4 says nothing detects.
  - **The candidate fix is one wake per queue the script touched,** not one per message. The readiness is
    per queue, and a wake's `key` field is only for humans, so nothing reads it. A batch of 1,000 messages
    across 3 queues would then cost 3 entries.
- **To settle:**
  - the slack's value;
  - whether the backoff mover and the schedule mover want different ones;
  - whether one wake per queue holds for the backoff mover too.

Nacks can follow the same design: with today's per-queue `delayed`, which costs no schema bump, or with a
per-partition set. They would use a separate mover instance (see the session log).

What the readyAt change was measured for is nacks only. There, readyAt is stored with the key and delays the
whole key. A scheduled message is one message, so the `{Q}:scheduled` set and its promotion by the claim
remain a sketch, and nothing has measured them.

### 4. What does the message id mean for a scheduled message?

D5 says the same id twice for one key is one message "for as long as the first is still queued". The
questions:
- Is a scheduled message "queued" for that purpose?
- What happens when an ordinary enqueue arrives with the id of a scheduled one?
- What happens when a schedule repeats an id: is it a no-op, or does it move the due time?

**Reschedule-by-id** (same id, new time, replace) is a cheap and useful form of *cancel and replace*. The
backstop nudge wants exactly that: push the deadline out when the conversation moves.

### 5. Is cancelling part of the verb?

- An explicit `Unschedule(queue, key, message_id)`. Azure Service Bus returns a sequence number from a
  schedule for this purpose (from memory; to verify).
- Reschedule-by-id with a time in the past, which means "deliver now".
- Nothing, because a nudge is idempotent and a spare one costs one empty turn.

### 6. The RPC's shape

- **A new verb.** `Schedule(ScheduleRequest { queue, message, deliver_at })` rather than an optional field
  on `Enqueue`. That was the stated intent, and it keeps `Enqueue`'s contract ("returns once it is durably
  queued") unchanged.
- **An absolute time or a delay.**
  - `deliver_at` is a `Timestamp` from the caller's clock, compared against the store's.
  - `deliver_after` is a `Duration` measured on the store's clock. It is immune to client skew, but a
    retried request moves the due time.
  - Possibly both, as a `oneof`.
- **Bounds.**
  - A horizon (`max-schedule-ahead`) keeps the set from growing without limit.
  - A due time in the past is either refused or treated as an ordinary enqueue.
- **What the response says.** The due time as recorded, after clamping. Perhaps whether the request
  replaced an earlier schedule.

### 7. What it costs to deploy

New structures that an old instance does not read are not a misread, so this may not need a schema version
bump ([`schema-versioned-keys.md`](schema-versioned-keys.md)). During a rolling deploy, old instances would
simply not promote, and new ones would. To confirm against the bump rule in
[`redis-cluster.md`](../architecture/redis-cluster.md).

### 8. What the guarantees page would gain

- **S1.** A scheduled message is never claimable before its due time, on the store's clock.
- **S2.** After its due time it is an ordinary message: O1–O4 and D1–D5 apply from then on.
- **S3.** Lateness is bounded by whatever question 3 decides.

The page's current "no deadline scheduling" line becomes "no priority".

## Research rounds

1. **Prior art.** Sidekiq's scheduled set and poller (it randomises its poll interval across processes),
   BullMQ's delayed set, SQS message timers and their 15-minute cap, Azure Service Bus scheduled messages
   and cancel, the RabbitMQ delayed-message exchange, Pulsar's `deliverAt`. For each: where the message
   waits, how lateness is bounded, what an id means, how cancel works.
2. **Decide 1, 4 and 5**, the semantics, with the agent consumer's backstop as the worked example.
3. **Decide 3**, lateness. Measure what the watchdog tick gives, and decide whether `delayed` should share
   the precise timer.
4. **Sketch the scripts and the RPC**, with the claim path untouched, and list the `QueueStoreSpec` and
   e2e cases (several of them need a real clock).
5. **Settle the deploy question** (7) and the guarantees text (8).

## Out of scope here

- **Recurring schedules** (cron). A consumer can reschedule its own next nudge as it handles the current
  one.
- **Per-message deferral of a message already queued.** That is the other verb in
  [`two-verbs-an-agent-consumer-asked-for.md`](two-verbs-an-agent-consumer-asked-for.md), with its own
  reasons to wait.
- **Priority.**
