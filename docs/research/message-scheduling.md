---
title: "Message scheduling — a message that becomes claimable at a time (pre-research)"
type: research
status: draft
updated: 2026-11-14
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

**Decided (2026-11-14): how a member of `{p}:scheduled` is encoded.** With the per-partition set (question
3), a member names the queue, the key and the message id, and it is also the field of
`{p}:scheduled:payloads`. Validation only requires the three to be non-empty, so any of them may contain any
character, and no separator is safe. The member is therefore **length-prefixed, id last**:

```
<byte length of queue>:<queue><byte length of key>:<key><message id>
6:orders3:c42nudge
```

- Unambiguous for any content, readable in `valkey-cli`, and split in Lua with `string.find` (plain),
  `tonumber` and `string.sub`. The id takes what is left, so it needs no prefix.
- **The lengths are UTF-8 byte lengths,** since Lua's `#` and `string.sub` count bytes. The Scala encoder
  measures `getBytes(UTF_8).length`, not `String.length`, which counts UTF-16 code units.
- A separator was rejected: it would need a new rule forbidding it in queue names, keys and ids, a
  contract change for `Enqueue` too. `cmsgpack` was rejected: opaque in `valkey-cli`, for no gain.
- Chosen once: changing it after release is an encoding changing form, so a schema bump (question 7).

**Ties are broken by id.** A sorted set orders equal scores by member bytes, so two messages of one key due
in the same millisecond move in id order, not in the order they were sent. Two messages due at the same
instant have no natural order (question 1(a)), so a deterministic one is enough. Ordering by send time
would need a sequence number in the member, which would stop the member from being the message's identity,
and question 4's first-wins check (`ZADD NX` on the member) would then need an index of its own.

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

D5 says the same id twice for one key is one message "for as long as the first is still queued". A
scheduled message adds a second place where a message with an id can wait, so the question is how the id
behaves across the two.

**Decided (2026-11-14):**

- **A schedule that repeats an id still scheduled is ignored: the first one wins.** Rescheduling is not
  offered. What is advertised to move a deadline is a new message, with its own id and its own schedule.
- **An id is only compared when the message joins its key's line.** A scheduled message is not "queued" for
  D5's purpose until it is moved. An ordinary enqueue with the id of a scheduled message is not advertised,
  but it is handled like any enqueue: it is queued at once. When the scheduled message falls due, the mover
  applies the normal enqueue logic: if a message with that id is still queued for the key, the scheduled
  one is discarded; otherwise it is queued. The same holds the other way round, for a schedule whose id is
  already queued.

**Why it costs nothing extra:**

| Path | What runs | Extra cost |
|---|---|---|
| Schedule | `ZADD NX <member> <due>`, then `HSET` of the payload only when that added it | none: `NX` is the same O(log N) insert; an ignored schedule skips the payload write |
| Enqueue | unchanged | none: it never looks at the scheduled set |
| Move, per due message | `ZREM` the member, read and `HDEL` its payload, then the enqueue logic (`HSETNX` on the key's `payloads`) | none: the duplicate check is that `HSETNX`, O(1), as in every enqueue; a discard skips the `RPUSH`, the `ZADD ready` and the wake |

What does weigh on performance is unchanged by these rules and already listed under question 3: large batches
due together, and the memory of schedules far ahead (bounded by the horizon in question 6).

**Consequences:**

1. **A retried `Schedule` is harmless whatever the time format.** It is ignored, so `deliver_after` is as
   safe as `deliver_at`. Question 6 can choose between them on other grounds, such as clock skew.
2. **The backstop nudge receives stale nudges.** With no reschedule and no cancel (question 5), every earlier
   nudge still fires. The consumer recognises a stale one itself, for instance from a deadline carried in
   its payload. Question 5 already accepts that a spare nudge costs one empty turn; the consumer's docs must
   say it.
3. **The response says what was recorded.** When the id was already scheduled, the answer carries the
   existing due time, so a caller sees that its own time was not applied, without an error. A detail of
   question 6.
4. **A discard at move time is counted.** A metric for "discarded when due, id still queued" makes the
   silent merge visible. The script already knows, so it is free.

The options as first listed, for the record:
- Is a scheduled message "queued" for that purpose?
- What happens when an ordinary enqueue arrives with the id of a scheduled one?
- What happens when a schedule repeats an id: is it a no-op, or does it move the due time?

**Reschedule-by-id** (same id, new time, replace) was the leaning before the decision: a cheap form of
*cancel and replace*, which the backstop nudge would use to push its deadline out. It was dropped in favour
of a new message per deadline, which keeps the identity rule the same everywhere: the first one wins.

### 5. Is cancelling part of the verb?

**Decided (2026-11-14): no. Cancelling is deferred, as a possible future feature.** The first version ships
`Schedule` alone.

The options, kept for when it comes back:
- An explicit `Unschedule(queue, key, message_id)`. Azure Service Bus returns a sequence number from a
  schedule for this purpose (from memory; to verify).
- Reschedule-by-id with a time in the past, which means "deliver now". No longer available: question 4
  decided that a repeated schedule is ignored.
- Nothing, because a nudge is idempotent and a spare one costs one empty turn.

**Why adding it later stays cheap:** with the per-partition layout, a member of `{p}:scheduled` *is*
`(queue, key, message id)`, so an `Unschedule` would be one script (`ZREM` and `HDEL` on that member), with
no index to add. Its race with the mover would be clean: both are scripts on the partition's slot, so one of
them wins, and a message the mover has already moved is an ordinary message that `Unschedule` would leave
alone.

### 6. The RPC's shape

- **A new verb.** `Schedule(ScheduleRequest { queue, message, deliver_after })` rather than an optional
  field on `Enqueue`. That was the stated intent, and it keeps `Enqueue`'s contract ("returns once it is
  durably queued") unchanged.
- **Decided (2026-11-14): a delay, `deliver_after`, a `google.protobuf.Duration`.** The script adds it to
  the store's own clock (`TIME`), so the due time never depends on the caller's clock or on any instance's.
  `deliver_at` (a `Timestamp` from the caller's clock) was dropped: comparing it against the store's clock
  raises the question of clock synchronisation, which the delay avoids.
  - **A retry does not move the due time.** That was the delay's drawback, and question 4's first-wins rule
    removes it: the retried request repeats the id, so it is ignored.
  - **The delay runs from when the script runs,** not from when the caller sent it, so network and queueing
    time add to it. For a backstop measured in minutes, that is noise.
- **Decided (2026-11-14): the delay is required and must be greater than zero.** An unset `Duration` reads
  as zero, so leaving it out, zero and a negative delay are all refused as a `ValidationError`
  (`INVALID_ARGUMENT`): the same rule as `DequeueRequest.max_wait`. "Deliver now" is what `Enqueue` is for.
  The message itself is validated exactly as for `Enqueue`.
- **Decided (2026-11-14): the horizon is a deployment setting.** `max-schedule-ahead` caps
  `deliver_after`, and a delay beyond it is refused as a `ValidationError` naming the limit (not clamped,
  which would deliver early in silence). Its value is the deployer's decision, not DKQ's.
  - **Why it exists at all:** a scheduled payload stays in Valkey's memory until it is due, and nothing
    drains it earlier. Persistence (RDB, AOF) only keeps a copy on disk for recovery; it adds no capacity.
    On a `noeviction` instance, full memory refuses *every* write, enqueues and settles included. Memory
    held by schedules is roughly rate × payload size × delay, and a unit mistake in a producer (seconds for
    milliseconds) is the realistic way it grows. A horizon bounds how long a schedule stays, not how many
    there are.
  - **Default: 7 days** (decided 2026-11-14). Generous for a backstop measured in minutes or hours, and
    still short enough to turn a unit mistake into an immediate `INVALID_ARGUMENT`.
- **Decided (2026-11-14): the response.** `ScheduleResponse { bool scheduled; google.protobuf.Duration due_in; }`:
  - `scheduled` is false when the id was already scheduled, so this request was ignored (question 4);
  - `due_in` is how long until the recorded schedule is due, the existing one's remaining time when the
    request was ignored. Computed on the store's clock, so no absolute time crosses the API.

### 7. What it costs to deploy

**Decided (2026-11-14): adding structures needs no schema bump; changing an existing one does.** This is
the bump rule of [`redis-cluster.md`](../architecture/redis-cluster.md) applied as stated there: the version
moves "whenever an older instance would misread the store", and a key an older instance never reads cannot
be misread.

Applied to this feature:
- **New keys only:** `{p:<n>}:v3:scheduled` (zset) and `{p:<n>}:v3:scheduled:payloads` (hash), at partition
  level like `{p:<n>}:v3:wake`. No bump.
- **Existing structures, written in their existing form:** the mover writes a key's `payloads`, `msgs`,
  `ready` and the wake stream exactly as `enqueue.lua` does. No bump.
- **The one condition:** nacks keep today's per-queue `{Q}:delayed`. Question 3's per-partition
  `{p}:delayed` for nacks would change an existing structure's place and member form, so it would need a
  bump. That belongs to the nack mover, not to this feature.

During a rolling deploy:
- an old instance has no `Schedule` RPC (a call routed to it gets `UNIMPLEMENTED`) and runs no mover; the new
  instances move every due message, whichever instance accepted it;
- a full rollback leaves schedules waiting, not lost: they stay in `{p}:scheduled` until a version with the
  mover runs again, then arrive late.

**The member encoding is chosen once.** It is new, so it can be anything now, but changing it after release
is an encoding changing form, so a bump.

### 8. What the guarantees page would gain

- **S1.** A scheduled message is never claimable before its due time, on the store's clock.
- **S2.** After its due time it is an ordinary message: O1–O4 and D1–D5 apply from then on.
- **S3.** Lateness is bounded by whatever question 3 decides.
- **S4.** A scheduled message is as durable as the store's persistence, no more. With persistence off (as in
  the compose stacks), a Valkey restart loses it; with AOF and `appendfsync everysec`, at most about a
  second of writes. Which one is the deployer's decision. Scheduled messages are the most exposed to it,
  because they live hours where a queued message lives seconds.
- **S5.** Messages of one key due in the same millisecond join its line in message-id order, not in the
  order they were sent.

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

- **Recurring schedules** (cron). A consumer can schedule its own next nudge, as a new message, as it
  handles the current one.
- **Per-message deferral of a message already queued.** That is the other verb in
  [`two-verbs-an-agent-consumer-asked-for.md`](two-verbs-an-agent-consumer-asked-for.md), with its own
  reasons to wait.
- **Priority.**
