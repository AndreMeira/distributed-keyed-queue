---
title: "2026-11-02 — the next two features: message scheduling, and key stickiness"
type: session
status: current
updated: 2026-11-02
tags: [roadmap, schedule, stickiness, affinity, hooks, pre-research]
---

# 2026-11-02 — the next two features

After the review-thread cleanup (`2026-11-02-old-review-threads.md`), the focus moves to new feature work.
There are two features to evaluate. Each has a pre-research note, and nothing is decided yet: the next step
is to talk them through.

## 1. Message scheduling (medium)

**Goal:** a new verb on the service that sends a message now and makes it claimable at a later time. From
its due time it is an ordinary message of its key.

Its first consumer is the agent loop's backstop nudge from
[`two-verbs-an-agent-consumer-asked-for.md`](../research/two-verbs-an-agent-consumer-asked-for.md).

Pre-research: [`message-scheduling.md`](../research/message-scheduling.md). The question that decides the
rest is where a scheduled message takes its place in its key's order. The note leans to "when it becomes
due". The other questions:
- how late a due message may be: the 5 s watchdog tick, or a precise timer;
- what a repeated id means, and whether rescheduling by id is the cancel;
- who sweeps a queue after a full restart.

### First, the backoff path: `ready` scored by readyAt

Talking through how the watchdog releases a nack's `retry_after` showed that a 1 s backoff can wait up to
about 6 s. The watchdog only looks every `sweep-interval`, and only at queues its own instance has served.

The proposal is to score `ready` by when a key becomes claimable, so a backoff is a future score and
`delayed` folds into `ready`. Nothing then has to happen at the due time. An empty claim reports the next
due time, and the readiness keeps a timer for it.

Pre-research: [`ready-scored-by-readyat.md`](../research/ready-scored-by-readyat.md). Two things to solve:
- ties, which the counter fixed: the candidate is a monotonic microsecond clock;
- a backoff asked for while the claim is still alive: a small `{Q}:retry` hash until the claim ends.

It is a schema bump to v4. Scheduling would build on it, so it would go first.

**Evaluated, and viable for nacks.**
- **Microbenchmark** (throwaway Valkey 8.1):
  - A sorted set's push and pop cost the same time as a list's from 1,000 keys up. Its price is memory:
    about 87 bytes per claimable key against 17, already paid since `ready` became a sorted set.
  - The readyAt claim adds about a microsecond inside Redis, or nothing on a claim that finds work if it
    pops the head and puts it back when not due.
- **Throughput sweep:** a slice built only for the benchmark, and thrown away afterwards, ran interleaved
  with `main`, without nacks. No difference stands out from run-to-run noise.

The numbers are in the note's "Complexity" section.

**What the full switch would take:** about two days. The scripts, the key layout, the codecs and the port
are easy. The readiness timer is the real work, with its fit to the seed token, the hand-on and the
cancellation hand-back.

Two names wait for approval: `{Q}:clock`, replacing `{Q}:seq`, and `{Q}:retry`, the hash holding a backoff
asked for during a live claim.

**It says nothing about scheduled messages.** Here readyAt is stored with the key, so it delays the whole
key. A scheduled message is one message, and its key's other messages must stay claimable meanwhile. So
where a scheduled message waits, and how it reaches the key's line when due, is still open:
- the `{Q}:scheduled` set promoted by the claim, sketched in the note, which nothing has measured;
- or a design that keeps readiness per message.

Only the timer could be shared between the two features.

Also found while measuring: Valkey 8.1 freezes `TIME` for the whole script. Every call inside one script
returns the same value.

### Then: a tick-driven mover instead of the watchdog's fixed sweep

A second strategy came out of the same discussion, and it is the one now preferred for both nacks and
scheduled messages.

**How it works:**
- Each instance runs a mover driven by a ticker.
- A tick runs a script per queue that moves what is due (held-back keys into `ready`, and later scheduled
  messages into their key's `msgs`), appends a wake for each, and returns when the next thing is due. The
  mover asks the ticker for a tick then.
- The instance that receives a nack with `retry_after`, or a scheduled message, asks its own ticker for an
  early tick at the due time.
- A periodic backstop tick on every instance covers a request lost with an instance (a crash, a restart).

**Why it beats readyAt for precision:**
- The move is a write again, so it appends the wake, and the readiness and `DequeueUseCase` don't change.
- For nacks, `delayed` can stay as it is: no schema bump and no new structure.
- readyAt scoring stays possible, but only to remove `delayed`, no longer for precision.

**Gaps found:**
- **The safety net covers only queues the surviving instances have served** (`Watchdog.queues`, in
  memory since boot). A per-partition index of queues with something waiting would make it cover every
  queue.
- **A tick sweeps every known queue,** not just the one that is due.
- **Early ticks are lost on a rolling deploy,** so pending backoffs wait for the backstop.

**The ticker itself.** Two versions were written in the toolkit's incubator, not committed yet:
- **`time/v1/Ticker`** is the user's. A tick swaps a promise and reaches whoever is waiting at that moment.
  It is an edge-triggered broadcast, and a reasonable toolkit primitive whose doc must say that a consumer
  not waiting misses the tick. The fixes made today:
  - an initial offer in `make`;
  - the version read at tick time, so an early `next` no longer kills the backstop chain;
  - the backstop scheduled after `period`.
- **`time/v2/Ticker`** has one consumer, the earliest requested instant wins, and a `sliding(1)` nudge
  remembers a request made while the consumer is busy. It is level-triggered.

**Decided:**
- **The mover needs the level-triggered version.** A tick missed mid-sweep can be the only record of a nack
  the sweep did not see, which costs up to a full `period`.
- **So v2 belongs in DKQ,** in `domain/service/maintenance/` next to `Watchdog`, not in the toolkit. v1
  stays in the incubator.

**Decided: scheduled messages wait in a sorted set.** They are persisted the moment they arrive, and the
ticker only makes the move early.

**Open: per queue or per partition.** At first, per partition was chosen "because a per-queue set is not
found after a full restart". That reason does not hold. A move only matters to someone who dequeues the
queue, and a dequeue registers the queue on the instance that serves it. So both strategies are correct,
and they trade off:
- **Per queue** (`{Q}:scheduled`, and today's `{Q}:delayed`):
  - a tick runs one script per known queue;
  - registering a queue must sweep it at once, or fetch its next due time;
  - nacks need no schema bump;
  - after a full restart, an idle queue is moved when its first consumer returns.
- **Per partition** (`{p}:scheduled`, `{p}:delayed`):
  - a tick runs one script per partition, whatever the number of queues;
  - no registration is needed;
  - any backstop moves due work, even on an idle queue;
  - nacks need a new structure and a schema bump.

Per partition wins with many queues. Per queue changes nothing for nacks.

**Preferred afterwards, still under evaluation: per partition, for both** (`{p}:scheduled`,
`{p}:delayed`). The work per tick is
bounded and small, one script per partition, and the design is simpler: no registration and no list of
known queues. The costs:
- a sweep limit shared by a partition's queues;
- a schema bump for nacks.

Still to handle, either way:
- clock skew, by returning "due in X ms" on the store's clock;
- every instance sweeping at the same due time;
- the place in the key's order being taken at move time;
- large batches due together.

To evaluate, not decided:
- **Batching through the slack.** A slack of about 50 ms collapses nearby requests into one tick, whose
  script moves everything due in the partition at once, for up to the slack in lateness.
- **One wake per queue touched, not one per message.** Otherwise a large batch could flood the partition's
  stream, which is trimmed at about 1,000 entries.

Details are in `message-scheduling.md`.

Nacks follow the same design, but **not the same mover instance**. The plan is one mover written once and
two instances, each with its own ticker, period, slack and limit:
- one over the held-back keys, moving them into `ready`;
- one over the scheduled messages, moving them into their key's `msgs`.

The reasons:
- backoffs (seconds) and schedules (hours) want different backstops;
- a burst of schedules due together must not hold up backoff releases;
- each gets its own limits, logs and metrics;
- each script makes one kind of move.

**Pending:**
- the DKQ class's name: `Ticker` fits the toolkit primitive, not the mover's timer;
- moving v2 out of the toolkit (it is staged on #64's branch, `recursion-doc-corrections`, and must leave
  it), with its tests;
- v1's contract doc and tests, on a branch of its own.

### The mover's architecture, written up (2026-11-03)

The ticker became a one-shot `TimeLatch` (toolkit incubator, `time/v3`, not committed yet). It is created armed and never
reset: a tick source owns it, replaces it on every fire, and emits a tick through a `sliding(1)` pipe to the
mover, which feeds the next due time back.

The full design, still exploration: [`tick-driven-mover.md`](../research/tick-driven-mover.md).

### Found in review (2026-11-06): the due sweep publishes a key that is still claimed

Copilot's review of the mover note found a gap that today's watchdog has too.
- **What happens:** a partial nack with `retry_after` writes `delayed` while the rest of the batch is
  still owned, and `settle.lua` returns with the claim alive. If the backoff ends first, `sweep.lua`'s due
  sweep runs `ZREM delayed`, `ZADD ready`, `XADD wake` without checking `claimed`.
- **The result:** another consumer's claim pops the key, overwrites the lease and advances the fence. It
  receives the messages the first consumer is still working, and that consumer's remaining settles are
  refused. E1 breaks.
- **Who is exposed:** the managed client settles a batch in one call, so its claims end in the same
  script and it is not exposed. A consumer settling piece by piece through the raw API is.
- **The fix:** remove the elapsed delay, and publish only a key absent from `claimed`. The claim's end or
  a reclaim publishes it otherwise. It ships on its own PR, with a regression test.

## 2. Key stickiness (larger, several research rounds)

**Goal:** messages of one key go preferably to the same consumer pod, a little like partition assignment
to a Kafka consumer group. The consumer also gets hooks, or another way to detect, when it gains or loses
a key or a group of keys.

Pre-research: [`key-stickiness.md`](../research/key-stickiness.md). Strategies to explore:
- **A.** Shard the queues themselves (`orders.2`), with shard ownership through dkq's own lock API.
- **B.** Soft affinity in the store: remember each key's last holder.
- **C.** Rendezvous hashing over the live consumers.
- **D.** Leased slices, the rewrite sketched in `dequeue-latency.md`.
- **E.** Session-style claims.

The fact that shapes them is that per-key exclusivity is already enforced by the store, so stickiness
only has to be mostly right.

The first rounds would be:
1. measure today's accidental stickiness;
2. read the prior art;
3. prototype strategy A in the client, without a server change.

## Next

Talk through both notes, then pick which feature goes first.
