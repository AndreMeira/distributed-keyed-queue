---
title: "Connections and threads: why a synchronous Redis client is the right call here"
type: architecture
status: current
updated: 2026-09-05
tags: [redis, lettuce, connections, threads, zio, latency, measurement]
---

# Connections and threads

Two connections, a synchronous client, and `ZIO.attemptBlocking` around every call. All three look like
things to apologise for, and this page is the measurement that says they are not.

## The model

`Connection` holds exactly two:

- **A shared one** for everything that answers immediately — every claim, settle, heartbeat and sweep is a
  single `EVALSHA`. Lettuce connections are safe to use from many threads and pipeline what they are given,
  so one serves the whole instance.
- **An exclusive one** for the listener's `XREAD … BLOCK`, which parks for as long as it is told. Sharing it
  would put every claim and settle behind that read, and its command timeout is deliberately set above the
  longest block so Lettuce does not abandon a read that is doing what it was asked to.

Nothing else is pooled, because nothing else waits.

## What the parts actually cost

Measured on this laptop, not estimated.

**Redis executing a script: ~4µs.** `produce.lua` — the heaviest write path, five keys plus the `XADD` —
benchmarked over 20,000 `EVALSHA` calls against Valkey 8.1:

```
cmdstat_evalsha: calls=20000, usec_per_call=4.18
latency_percentiles_usec_evalsha: p50=4.0, p99=16.1, p99.9=26.1
```

**A round trip from a client in the same container: ~55µs**, 18,570 requests/second on one connection. So
the socket and the client library are already an order of magnitude more than Redis's own work.

**The blocking-pool hop: ~0.3µs.** 200,000 iterations of each, single fiber:

```
plain ZIO.succeed      0.08 us/op
ZIO.attemptBlocking    0.40 us/op
```

**The observed request: 6–7ms**, from the end-to-end suite. Against which Redis is 0.06% and the
`attemptBlocking` ceremony is 0.005%. Almost all of it is transport — in that harness, chiefly the gRPC hop
from a host JVM into a container across Docker Desktop's VM boundary.

Throughput confirms it is transport rather than work: **~4,000 msg/s at ~7ms latency means about 28
requests in flight**, not seven milliseconds of computation each. Seven milliseconds of CPU per request
would need dozens of busy cores to reach that rate.

## So what does the synchronous client cost?

Not CPU. **A parked thread per in-flight Redis call** — the thread waits out the round trip rather than
doing anything. The question is therefore how many calls are in flight at once, and the answer is bounded by
concurrency, not by consumers:

- A consumer **waiting** for work holds no thread and no connection. It waits on a promise in `Waiters`, and
  is woken by the listener. This is the part that changed: under the old `BLMOVE` design a parked consumer
  held a thread *and* a connection for its whole patience — up to thirty seconds of doing nothing.
- A consumer **claiming** holds a blocking thread for the duration of one script — microseconds of Redis
  work plus the network. At the shapes measured here, that is a few dozen threads at peak.

Lettuce's synchronous API is implemented over its asynchronous one — it issues the command and awaits the
future — so the Netty-to-caller handoff exists either way. Going asynchronous removes the parked thread, not
the handoff.

## When to revisit

Switch to `RedisAsyncCommands` with `ZIO.fromCompletionStage` when either of these becomes true, and not
before:

- **Hundreds or thousands of concurrent Redis operations per instance.** A thread each is about a megabyte
  of stack plus scheduler pressure; that is the real ceiling of the current approach.
- **A measurement showing server-side cost matters.** Today's numbers cannot show it: the Docker Desktop
  boundary hides everything smaller than a millisecond. Run the load generator *inside* the compose network
  first — whatever remains after that boundary is gone is the honest server cost.

The listener would keep its own connection regardless. Redis serves one connection's commands in order, so a
blocking `XREAD` occupies the connection whether or not it occupies a thread.

## Where the numbers came from

- Script cost: `valkey-benchmark` driving `EVALSHA` of `produce.lua`, then `INFO commandstats` and
  `INFO latencystats` for the server-side split.
- Hop cost: 200,000 iterations of `ZIO.attemptBlocking(())` against `ZIO.succeed(())` in a scratch spec.
- Request latency and throughput: the opt-in indicators in `ThroughputSpec`, recorded in
  [`../research/throughput-first-numbers.md`](../research/throughput-first-numbers.md).
