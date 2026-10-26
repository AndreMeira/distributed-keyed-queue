---
title: "Connections and threads: an asynchronous shared connection, synchronous partition ones"
type: architecture
status: current
updated: 2026-10-26
tags: [redis, lettuce, connections, threads, zio, latency, measurement]
---

# Connections and threads

One shared connection plus one per partition — two in all on a single server, which uses a single
partition; seventeen on a cluster, which uses sixteen. A blocked read occupies its connection whole, so the
listener cannot share. All of them are opened by `Connection.make` at startup, so the count is fixed before
anything serves.

The shared connection is used through Lettuce's asynchronous API: every call answers a future, awaited with
`ZIO.fromCompletionStage`, so a call in flight parks a fiber rather than a thread. The partition connections
keep the synchronous API under `ZIO.attemptBlocking`, because their `XREAD … BLOCK` occupies the connection
whichever API issues it. The shared connection was synchronous too until 2026-10-26; the measurements below
are from that time, and still describe what each part costs.

## The model

`Connection` holds one shared connection and one per partition:

- **A shared one** for everything that answers immediately — every claim, settle, heartbeat and sweep is a
  single `EVALSHA`. Lettuce connections are safe to use from many threads and pipeline what they are given,
  so one serves the whole instance.
- **One per partition** for the listener's `XREAD … BLOCK`, which parks for as long as it is told. Sharing
  would put every claim and settle behind that read, and its command timeout is deliberately set above the
  longest block so Lettuce does not abandon a read that is doing what it was asked to. One per partition
  rather than one for all of them because a partition's keys are a slot of their own on a cluster, and a
  command may not span slots — so each reads exactly its own partition's wake stream.

Nothing else is pooled, because nothing else waits.

## What the parts actually cost

Measured on this laptop, not estimated.

**Redis executing a script: ~4µs.** `enqueue.lua` — the heaviest write path, five keys plus the `XADD` —
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

## What the synchronous client cost, and why the shared connection left it

Not CPU. **A parked thread per in-flight Redis call** — the thread waits out the round trip rather than
doing anything. The question is therefore how many calls are in flight at once, and the answer is bounded by
concurrency, not by consumers:

- A consumer **waiting** for work holds no thread and no connection. It waits for a token in `QueueReadiness`, and
  is woken by the listener. This is the part that changed: under the old `BLMOVE` design a parked consumer
  held a thread *and* a connection for its whole patience — up to thirty seconds of doing nothing.
- A consumer **claiming** holds a blocking thread for the duration of one script — microseconds of Redis
  work plus the network. At the shapes measured here, that is a few dozen threads at peak.

Lettuce's synchronous API is implemented over its asynchronous one — it issues the command and awaits the
future — so the Netty-to-caller handoff exists either way. Going asynchronous removes the parked thread, not
the handoff: the ceiling it lifts is the one at hundreds or thousands of concurrent Redis operations per
instance, a thread each. That is the change that was made, on the shared connection only.

## What the asynchronous shared connection keeps from the synchronous one

Two properties came for free with `attemptBlocking` and are kept deliberately:

- **A call that has been sent is waited for.** Interrupting a fiber blocked in `attemptBlocking` does not
  interrupt the thread, so the fiber always learned what its script did. A future can be abandoned instead,
  and then Redis would still run the script — a claim or a lock grant made for a fiber that has left. So
  `LuaScript.runScript` awaits the reply uninterruptibly: the wait parks the fiber, not a thread, and the
  caller still sees every script it sent.
- **A server that stops answering fails the call.** The connection's command timeout bounds the
  asynchronous API too: Lettuce's default client options (`TimeoutOptions.enabled()`) expire a command after
  the connection's own timeout, which `Connection.connect` sets. Without that, an uninterruptible wait on a
  frozen server would never end.

`ConnectionSpec` pins both against a server frozen with `CLIENT PAUSE`.

The listener keeps its own synchronous connections. Redis serves one connection's commands in order, so a
blocking `XREAD` occupies the connection whether or not it occupies a thread.

## Where the numbers came from

- Script cost: `valkey-benchmark` driving `EVALSHA` of `enqueue.lua`, then `INFO commandstats` and
  `INFO latencystats` for the server-side split.
- Hop cost: 200,000 iterations of `ZIO.attemptBlocking(())` against `ZIO.succeed(())` in a scratch spec.
- Request latency and throughput: the opt-in indicators in `ThroughputSpec`, recorded in
  [`../research/throughput-first-numbers.md`](../research/throughput-first-numbers.md).

What these numbers imply about where latency could be *removed* — rather than where it is spent — is
[`../research/dequeue-latency.md`](../research/dequeue-latency.md).
