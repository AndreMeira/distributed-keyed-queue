---
title: "First throughput numbers — what they mean, and mostly what they don't"
type: research
status: draft
updated: 2026-08-29
tags: [throughput, measurement, load, e2e, keyed-queue, exploration]
---

# First throughput numbers

> **Not benchmark results, and not to be quoted as the throughput of this service.** They were taken out of
> curiosity, in one sitting, on one laptop, with every component — two service instances, Valkey, and the
> driving test JVM — on the same machine under Docker Desktop. Each run moved
> 256 messages in 57–205 ms, so timer granularity, GC and scheduler noise are all material at this scale.
> There is no rate control, no steady state, no percentiles, and a single trial of each shape would sit
> comfortably inside the spread of the three that were taken.
>
> They are useful for exactly one thing: **the shape of the curve** — which of the system's three ceilings
> binds first. Treat every absolute figure as an artefact of this laptop.
>
> For numbers worth quoting, point `ghz` at the stack, as `docs/architecture/end-to-end-testing.md` says.

## What was measured

The saturation check in `EndToEndSpec` ("under load nothing is lost…") re-run at five shapes, varying key
count and consumer count. The sweep was **temporary and has been reverted** — the repo carries no throughput
test. To rebuild it, see [Method](#method) below.

**The deployment is two instances**, `dkq-a` and `dkq-b` from `docker-compose.e2e.yml`, against one shared
Valkey; `Deployment` requires exactly two and fails otherwise, so every shape below ran against the same
pair. Consumers are spread round-robin across both, so the consumer column halves per instance — which
matters, because `claimers` is a per-instance setting.

Median of three runs per shape, after a warmup pass over every shape:

| keys | consumers | per instance | msg/s (median) | what bounds in-flight work |
|-----:|----------:|-------------:|---------------:|----------------------------|
| 8    | 8         | 4            | 1,552          | keys and consumers, both 8 |
| 16   | 16        | 8            | 1,896          | keys and consumers, both 16 |
| 64   | 8         | 4            | 2,048          | consumers |
| 64   | 16        | 8            | 3,160          | consumers |
| 64   | 32        | 16           | **4,339**      | consumers |

## What they show

**Consumers bind, not keys — provided keys ≥ consumers.** Holding consumers at 8 and taking keys from 8 to
64 buys +32%. Holding keys at 64 and taking consumers from 8 to 32 buys +112%. Per-key serialisation is the
design's central constraint, but it is not what limits this stack: it only matters that there are at least
as many keys as consumers, so none of them starve.

**`claimers` is not a throughput ceiling — and the sweep crossed it rather than inferring it.**
`claimers = 8` is per instance, so the last shape put 16 consumers against 8 claiming connections on each
instance: consumers genuinely contending, two deep. Throughput still rose ~37% over the 16-consumer shape,
which had one consumer per connection and no contention at all. The crossover happened inside the range
measured, so this is observed, not extrapolated.

The reason is that a claiming connection is held only for the duration of its `BLMOVE`, and on a queue with
work waiting that returns immediately — so `claimers` bounds how many dequeues can be *parked* at once, not
how much work can flow when there is work. Anyone tuning `claimers` to raise throughput on a busy queue is
tuning the wrong knob; it is the setting that decides how many idle consumers can wait.

**Per-message latency degrades as concurrency rises** — roughly 5.2 ms per message at 8 in flight, 7.4 ms at
32 — so throughput climbs sub-linearly and 32 consumers is somewhere near the knee *on this hardware*.

## What they don't show

- **A ceiling.** The sweep stopped at 32 consumers because that is where curiosity ran out, not because
  anything flattened. Where it actually tops out is unknown.
- **Anything about latency distribution.** No percentiles were taken. The per-message figures above are
  arithmetic on a mean, and a queue's interesting latencies are in its tail.
- **Anything under contention.** Every run drained a queue that was already full. Nothing here exercises
  producers and consumers competing, a slow handler holding a lease, or the watchdog sweeping under load.
- **Anything about the network.** Everything is loopback. gRPC over a real link is a different system.
- **Anything about durability cost.** Valkey here is a default container with no persistence tuning.

## Method

Two corrections were needed before the numbers meant anything, and both are the durable content of this
note:

1. **Time the drain, not the enqueue.** The load test starts its clock before enqueueing, and the enqueue
   phase is itself shaped by the configuration — 64 keys enqueue 64-way parallel, 8 keys enqueue 8-way with
   32 sequential each. Timing both together measures the producer's shape as much as the consumer's. The
   sweep enqueued first, then started the clock.
2. **Warm up, then repeat.** The instances are freshly started JVMs and the first shape pays for what the
   later ones enjoy. **In the first attempt this inverted a result**: 8 consumers appeared to beat 16, purely
   because it ran afterwards. A warmup pass over every shape, then three measured runs each, made the
   ordering effect disappear.

The same 8-key shape the committed load test uses reads ~1,550 msg/s under this method, against the ~429
msg/s the test itself printed in the same session — the difference is entirely cold JVM plus the enqueue
phase inside the timer. Note this when comparing against the 449 msg/s recorded in
`docs/learning-material/writing-end-to-end-tests.md`, which was measured the load test's way.

To rebuild the sweep: replicate the load test's body with the enqueue moved outside the timing window,
loop it over `(keys, consumers, perKey)` shapes holding total messages constant, run every shape once
untimed before measuring, and take three runs per shape.

## Why there is still no throughput test

Same reason `docs/architecture/end-to-end-testing.md` gives for the load test not being a benchmark: a
number without rate control, percentiles and a steady state invites exactly the misreading this warning
exists to prevent. If one is ever added it should be a separate, explicitly-run harness — not something
`sbt test` prints, where it would become a number people quote.

## Context

Taken on 2026-08-29, immediately after the adapter refactor that removed `Claimer`/`ClaimerPool`, folded
their registration bookkeeping into `RedisQueueStore`, and moved the heartbeat onto the shared connection.
That last change was made on reasoning rather than measurement, and the sweep is the only evidence so far
that it did not cost anything: results are in the same range as the load test's historical figures.
