---
title: "Checkpoint: telemetry works, dkq paused"
type: session
status: current
updated: 2026-09-06
tags: [checkpoint, observability, opentelemetry, demo, streaming, pause]
---

# Checkpoint: telemetry works, dkq paused

Written on pausing dkq to work on the toolkit's LLM module. The goal of the last stretch was narrow —
**prove traces and metrics work end to end** — and they do. What follows is what exists, what was learned,
and what to pick up first.

## What landed

Merged as `demo-app` (#6), on top of the sorted-set and error-redistribution work:

- **The agent ships in the image, off by default.** `sbt-javaagent` scoped `dist`, so `sbt run` never sees
  it and the packaged launcher carries the `-javaagent` flag itself. `dockerEnvVars` sets
  `OTEL_JAVAAGENT_ENABLED=false`; a deployment flips one variable.
- **`Monitor` is wired.** `measure` on all four RPCs in `QueueService`, `trace` on everything that reaches
  Redis in `RedisQueueStore`. `infrastructure/tracing/Module` binds the port to `OpenTelemetry.global`,
  which is a no-op SDK when no agent is attached — so the same wiring is correct in every environment.
- **A local stack**, behind a compose profile so `docker compose up` is still Valkey alone: Jaeger v2 as
  collector and trace UI, Prometheus, Grafana with both datasources and an overview dashboard provisioned.
  `bin/run.sh` brings up everything; `bin/demo.sh <scenario>` drives it.
- **`demo/`** — a scenario runner depending on `protocol-zio-grpc` and never on `server`, so it is evidence
  the published contract is usable. Three scenarios: `steady` (a baseline), `idle` (parked consumers, one
  message at a time), `flood` (producers flat out, consumers with no handler).

## What was verified

- Traces nest correctly, agent and manual spans in one tree:
  `KeyedQueue/Dequeue → QueueService.dequeue → RedisQueueStore.attempt → EVALSHA`. That also confirms
  `contextJVM` rather than the default fiber-local storage was the right choice — otherwise the two halves
  would form separate traces.

  > **Corrected later the same day.** This held only because the traces checked came from claims that never
  > parked. A thread-local does not survive a fiber park, so 99 of 100 `attempt` spans on waiting claims were
  > actually trace roots. The storage is now the `FiberRef`, with the agent's span adopted at the inbound
  > edge, and it lives in `homelab-telemetry` rather than here — see that repo's
  > `docs/learning-material/tracing-context-across-fibers.md`.
- Metrics reach Prometheus as `dkq_operation_hits_total`, `dkq_operation_latency_milliseconds_*`, tagged
  `operation`. The store's `trace` calls produce spans and no metrics, which is the trace-vs-measure split
  behaving as designed.
- The dashboard provisions from a committed JSON and its panels return data.

## What was learned, and what it costs

Numbers worth carrying forward — all in
[`../research/dequeue-latency.md`](../research/dequeue-latency.md), which is the note to read on resuming.

- **A unary RPC carries ~0.9ms of fixed overhead**, symmetric around the handler and unchanged by load or
  JVM warmth. **The agent adds ~1ms end to end** (A/B on `ThroughputSpec`'s round-trip indicator).
- **`dkq_operation_latency` is handler time, not call time** — 0.46ms recorded against 1.08ms the caller
  waited. A limitation to design out, not a bug.
- **Under `flood` the p99 is GC**, not dkq: 3.2ms mean young-generation pause, 157 collections in ~35s at
  ~4,000 msg/s. G1 is producing 3.2ms against a 200ms target, so there is nothing to tune and no reason to
  change collector — the live set is tiny because the state lives in Redis.
- **Below ~10ms the spans measure the runtime and the tracer as much as the work.** Some traces show a
  child outliving its parent, which is only possible if an end timestamp was recorded late; the drift
  matches the GC pause. Do not chase millisecond tails with this instrument.

One correction recorded because it was documented as fact and was wrong: the agent **does** instrument
zio-grpc's server. An early count over twenty traces showed no gRPC server spans; a later count over a
thousand showed them throughout.

## Where to resume

1. **Measure with the load generator inside the compose network.** Every latency estimate in the research
   note rests on a transport model dominated by Docker Desktop's host boundary, and this is the cheap step
   that either confirms it or invalidates the rest.
2. **Then streaming**, which is where the design is heading. `dequeue-latency.md` reaches it twice
   independently — from throughput (credits remove the ask-and-wait round trip) and from the per-call
   overhead measured here (a stream amortises the ~0.9ms). Step 1 in that note is the shape: credits over a
   bidirectional stream, no substrate change, backpressure preserved and made explicit.

## Loose ends

- **No collector in the cluster.** The agent would export nowhere; the compose stack is the only target
  that exists. Needed identically whether the agent ships in the image or an operator injects it.
- **The errors panel is unverified.** `dkq_operation_errors_total` and its `error_kind` label are inferred
  from the toolkit's instrument names — nothing has failed yet, so the series has never been created. A
  `failing` scenario would settle it, and is the obvious fourth scenario.
- **The histogram's top finite bucket is 10s**, against a `DKQ_MAX_WAIT` of 30s. An idle dequeue lands in
  `+Inf` and its percentile pins at exactly 10000ms — a plausible-looking number that cannot move. Needs an
  OTel view with explicit boundaries if dequeue latency is ever to be read seriously.
- **`demo/` has three scenarios**, and the ones that would exercise paths nothing currently watches are
  `hot-key`, `failing` and `slow`.
