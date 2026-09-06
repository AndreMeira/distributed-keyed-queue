---
title: Reading a latency tail — telling a GC pause from queueing, and what to do about it
type: learning-material
status: current
updated: 2026-09-06
tags: [performance, gc, g1, zgc, jvm, latency, tracing, allocation, measurement]
---

# Reading a latency tail — telling a GC pause from queueing, and what to do about it

dkq's dequeue latency has a tail: p50 around 1.2ms, p99 around 4.7ms, worst around 8ms at ~3,200 msg/s. This
is how that tail was identified as garbage collection, why swapping the collector made it *worse*, and what
the tail is actually telling you.

## Three signatures that say "stop-the-world", not "queueing"

The temptation is to read a p99 and start tuning. Bucket the requests by start time instead. Here is a
240ms burst at ~4,100 req/s, 5ms per bin:

```
   45ms  |  24 requests | p50  1.27  p99  1.82  max  1.82
   50ms  |  17 requests | p50  5.07  p99  6.58  max  6.58   <- p50 jumps 4.6x
   55ms  |   5 requests | p50  0.92  p99  2.07  max  2.07   <- throughput collapses
   60ms  |  19 requests | p50  1.17  p99  1.95  max  1.95   <- and recovers
```

Three things distinguish a pause from contention, and all three are visible:

- **p50 moves, not just p99.** Queueing stretches the unlucky request; a pause stretches *everything in
  flight simultaneously*. If the median of a time bucket jumps, the whole JVM stopped.
- **Throughput dips immediately after** — 5 requests against a ~21 baseline. Time was consumed, not
  redistributed. Queueing delays work; a pause destroys the interval.
- **Every component inflates together.** In the same data, `EVALSHA` (the Redis round trip) went p99 1.12 /
  max 4.57ms while our own code around it went p99 1.24 / max 4.30ms. If Redis were slow, the non-Redis time
  would not move; if our code were slow, `EVALSHA` would not. Both moving means the whole process froze,
  including the Netty threads waiting on the reply.

Confirm against the collector rather than stopping at the shape. Over 20 minutes: **144 young collections,
383ms total, mean 2.65ms**, p99 within the ≤10ms bucket. A ~4ms pause at t≈50ms produces exactly that bin.

One caveat that cost time: Prometheus scrapes every ~15s, so you cannot line up an individual 4ms pause with
a 5ms bucket from metrics. For per-pause timestamps use `-Xlog:gc:stdout:time,uptimemillis`, which prints
every collection with its duration and is free to leave on.

## Swapping the collector does not help

JDK 21, so generational ZGC is opt-in: `-XX:+UseZGC -XX:+ZGenerational`. Same 30s flood each time, 8
producers / 16 consumers / 512 keys:

| | throughput | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| **G1** (default) | 3,151/s | 1.27 | 1.98 | **4.74** | **8.34** |
| ZGC generational, default heap | 3,233/s | 1.32 | 3.31 | **20.66** | **44.52** |
| ZGC generational, `-Xmx512m` | 2,806/s | 1.42 | 2.51 | **8.36** | **10.82** |

ZGC is worse everywhere, and 4.4x worse at p99 until the heap is constrained. The default-heap run let the
heap balloon to 1.4GB where G1 sat at 125MB, and capping it recovered most of the damage — but even tuned,
ZGC is ~1.8x worse at p99 than G1 and gives up 11% throughput. There were **no allocation stalls** in either
run, so this is not the classic ZGC failure mode; it is ZGC's concurrent collection competing with
application threads for the same ten cores.

**Why it cannot win here.** G1's pauses over that run were 200 collections, 693ms total, mean 3.47ms. The
service turns its heap over roughly once a second. That makes the workload *allocation-rate-bound*, and
ZGC's entire trade is to spend CPU and throughput in order to shorten pauses. There is nothing for it to buy
back — the pauses are a symptom, and it treats the symptom.

## What the tail is actually telling you

200 collections in 30 seconds is the finding. It is set by how much garbage each request makes, which is
code, not a property of the JVM. Halve the allocation per request and the collection count roughly halves
with it.

So the levers, in order of size:

1. **Allocate less per request.** The real fix, and the one that needs an allocation profile
   (async-profiler in alloc mode over a flood) rather than reasoning. Guesses about which of the codec path
   or the Lettuce round trip dominates are just guesses.
2. **Turn the OpenTelemetry agent off when measuring.** It costs about 1ms per call — larger than the entire
   GC tail. Agent off moved a flood from 3,151 to 3,746 msg/s.
3. **Collector tuning.** Last, and possibly negative, as above. Swapping the *scheduler* is not on this
   list either: [`../research/kyo-scheduler.md`](../research/kyo-scheduler.md) records that experiment and
   why it could not be measured on this machine.

And keep the scale in view before spending any of that effort: p50 1.27ms, p99 4.74ms, worst 8.34ms. A tail
6x the median, entirely GC-shaped, on a default-tuned JVM, is unremarkable. What is *not* GC is the median —
a pause lands on about 1% of requests, so the median request never sees one. If the median is what you want
to move, the agent and the round trips are where it lives.

## One more trap in the same data

Some dequeue traces show a child span outliving its parent — `QueueService.dequeue` ending ~7.6ms after the
`KeyedQueue/Dequeue` that contains it. That is not clock skew and not GC: every one of those carries
`rpc.grpc.status_code: 1` (CANCELLED). When a client gives up, grpc-java closes the call and the agent stamps
the server span immediately, then ZIO interrupts the handler and our spans end in their finalizers a few
milliseconds later. Two actors ending two spans, in unwind order rather than tree order.

It shows up only on dequeue — 0 of 800 settles and enqueues — because dequeue is the only call that stays in
flight long enough for anyone to cancel it. Filter on the status code before reading latency percentiles, or
cancelled calls will quietly widen the tail you are trying to explain.
