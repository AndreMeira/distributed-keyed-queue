---
title: "What dkq reports about itself, and how to read it"
type: architecture
status: current
updated: 2026-09-11
tags: [observability, opentelemetry, metrics, tracing, latency, dashboards]
---

# What dkq reports about itself

Two sources, and they answer different questions. The **OpenTelemetry Java agent** instruments the
libraries — spans for every Redis command, JVM metrics — and it is in the image but off unless a deployment
enables it. The **`Monitor` port**, wired to that same SDK, instruments this service's own operations. How
the agent gets there and how it is switched on is
[`../learning-material/java-agents-and-telemetry.md`](../learning-material/java-agents-and-telemetry.md);
this page is about what the numbers mean.

## Where the instrumentation is

Every RPC is wrapped in `monitor.measure` at its handler — `QueueService` for the queue's four,
`LockService` for the lock's three — and nowhere else:

```scala
override def dequeue(request: v1.DequeueRequest): IO[StatusException, v1.DequeueResponse] =
  monitor.measure("QueueService.dequeue"):
    ...
```

A handler is the whole of one call — decode, use case, encode — so this times what the caller experienced,
and the four names are the four operations the API has. They are named after the RPC rather than the use
case because that is the surface a caller talks to; a second inbound adapter would measure its own.

Below the handler, the stores **trace but do not measure**: everything that reaches the substrate opens a
span, and nothing records a hit or a latency series. The RPC above already counts and times the operation a
caller asked for, so a second metric per store method would double the series for something the span
already answers — and the question these spans exist for is "where did that call's time go", which is a
trace question. Pure helpers are left alone: a span around clock arithmetic is noise. `RedisQueueStore.attemptClaim` is traced because it repeats — a claim that loses the race retries, and the
span count is what shows the redundant attempts.

The toolkit records three shared instruments, tagged with `operation`:

| metric | what it counts |
|---|---|
| `operation.hits` | calls started |
| `operation.latency` | milliseconds, as a histogram |
| `operation.errors` | failures, tagged also with `error.kind` |

One series per metric rather than one per operation, so a dashboard aggregates or breaks down with
`sum by (operation) (...)`.

## Reading the latency histogram

**`QueueService.dequeue` is not comparable to the other three, and a panel that treats all four alike will
be wrong about it.**

A dequeue that finds no work waits — deliberately, up to the caller's `max_wait`, clamped by
`DKQ_MAX_WAIT` (30 s by default). That wait is the feature: it is what makes a consumer's poll cheap and
what the whole wake path exists to shorten. So on an idle queue:

- `dequeue` p99 approaches `max_wait`, and **that is healthy**
- `dequeue` p50 near zero and p99 near `max_wait` is not bimodal noise, it is two populations: calls that
  found work immediately, and calls that waited out an empty queue

Reading that as a latency regression is the single easiest mistake to make with these numbers. Three ways to
avoid it, in increasing order of effort:

1. **Give `dequeue` its own panel.** Cheapest, and enough for a homelab: chart `enqueue`, `settle` and
   `heartbeat` together, and `dequeue` alone beside them.
2. **Alert on the other three only.** Any threshold that would be meaningful for `enqueue` is meaningless
   for `dequeue`, so an alert covering both is either deaf or noisy.
3. **Split the two populations.** What a consumer actually cares about is *"when work was there, how long
   until I got it"* — which is the wake-latency indicator in `ThroughputSpec`, not a percentile over all
   dequeues. Answering it in production would need the handler to tag the measurement with whether it
   returned a delivery, which the `Monitor` port supports (`tags`) but this service does not yet do.

The other three are ordinary: `enqueue` and `settle` are one Redis script each, `heartbeat` is one per
queue a consumer holds claims in.

## The lock's numbers

The lock's three RPCs are measured the same way (`LockService.acquire`/`release`/`refresh`), and
**`acquire` is `dequeue`'s sibling, caveat included**: a fair lock's acquire waits by design, up to the
caller's `max_wait`, so on a contended lock its p99 approaches `max_wait` and that is healthy. Give it its
own panel; alert on `release` and `refresh` only. The same two-population reading applies — grants that
were immediate, and waits that were served or timed out.

Beneath the handler, `RedisLockStore` traces what the wait was made of. The wait itself is
`LockAcquireUseCase`'s and opens no span of its own, so the caller's wait time is the handler's
measurement; what the store shows is each thing the wait asked for:

- **`RedisLockStore.place`** is the first ask — the lock, or a ticket for it.
- **`RedisLockStore.ask`** is one span per deliberate ask for the lock. Their count per acquire is the
  wake-efficiency signal: a waiter asks when a release wakes it, when a known deadline arrives (the lease's
  end, the head ticket's deadline), or when its patience runs out — an event or two each, never a poll's
  worth. A crowd of `ask` spans right after a release is the broadcast working: every local waiter asks
  once, the head wins.
- **`tryAcquire`, `release`, `refresh`, `trim`, `withdraw`** are one script each, ordinary latencies.

The cleanup loop follows the watchdog's rule: it runs on a timer (`DKQ_LOCK_TRIM_INTERVAL`), so what
matters is what it removed, which it logs — abandoned locks by name, because a holder that died without
releasing is an anomaly worth pointing at. `RedisLockStore.trim` spans exist for the call itself; the
interesting number is in the log line.

## What the agent adds, and what it misses

With `OTEL_JAVAAGENT_ENABLED=true` the agent contributes Lettuce spans — `EVALSHA` for every script call,
`XREAD` for the wake listener — plus JVM metrics. Verified against a running container: one enqueue
produced `EVALSHA` spans without a line of code being written for it.

Two gaps worth knowing before trusting a trace:

- **A Redis span made after a wait is its own trace.** The agent keeps context in a thread-local, which a
  fiber loses when it parks and resumes elsewhere, so `EVALSHA` from a claim that waited has no parent. Our
  own spans do not suffer this — `homelab-telemetry` keeps context in the `FiberRef` and bridges to the
  agent's span once per request — so `RedisQueueStore.attempt` sits under its `Dequeue` regardless of how
  long the claim waited. `docs/learning-material/java-agents-and-telemetry.md` has the full account.
- **`XREAD` spans are long and constant.** The listener's read blocks for `DKQ_WAKE_BLOCK`, so a
  multi-second `XREAD` span is the system idling correctly, not a slow query. The same caution as
  `dequeue`, one layer down.

The watchdog is not instrumented at all: it runs on a timer (`DKQ_SWEEP_INTERVAL`), and what matters about
it is what it repaired, which it logs. If it ever needs a metric, the count of reclaimed and released keys
is the useful one — not its latency.

## Turning it on

Nothing here reports unless the agent is enabled: `OpenTelemetry.global` is a no-op SDK otherwise, so
`measure` runs, costs nothing, and records nowhere. Locally that is `bin/run.sh`; in the cluster it is
`OTEL_JAVAAGENT_ENABLED=true` plus an OTLP endpoint on the deployment.
