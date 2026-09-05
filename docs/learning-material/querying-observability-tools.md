---
title: "Asking the observability stack a question from the terminal"
type: learning-material
status: current
updated: 2026-09-05
tags: [observability, prometheus, promql, jaeger, curl, metrics, tracing]
---

# Asking the stack a question from the terminal

The UIs are for looking around. When you want a specific number — *is this metric arriving at all? what are
the actual bucket boundaries? did that request produce a span?* — the HTTP APIs are faster and, more
usefully, quotable in a commit message.

Everything here assumes the local stack (`bin/run.sh`, or `docker compose --profile telemetry up -d`), which
maps Prometheus to `:9090`, Jaeger to `:16686` and Grafana to `:3000`. In the cluster the same endpoints sit
behind a Service; only the host changes.

## Prometheus

### The one call that answers "is anything arriving"

Before writing any query, find out what exists:

```bash
curl -s 'http://localhost:9090/api/v1/label/__name__/values' | jq -r '.data[]' | grep dkq
```

This lists every metric name Prometheus knows. It is the fastest way to tell an instrumentation problem
from a query problem — and the two look identical in a UI, where a wrong query and an absent metric both
draw an empty panel.

### An instant query

```bash
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=dkq_operation_hits_total'
```

**Use `--data-urlencode`, not a query string.** `/api/v1/query` accepts POST form data, so the PromQL goes
through unescaped. Hand-escaping it into a URL is how you get:

```
{"status":"error","errorType":"bad_data","error":"parse error: unexpected right parenthesis ')'"}
```

which reads like a broken query and is really a broken *encoding*. PromQL is full of `{`, `}`, `"` and
`()`; do not escape it by hand.

### Reading a histogram's shape

A histogram is a set of cumulative counters, one per bucket, labelled `le` ("less than or equal"). Asking
for the raw buckets tells you things a percentile never will:

```bash
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=dkq_operation_latency_milliseconds_bucket{operation="QueueService.dequeue"}' \
  | jq -r '.data.result[] | "\(.metric.le)\t\(.value[1])"' | sort -g
```

```
0.0       0
5.0       3
10.0      8
25.0      99
...
10000.0   986
+Inf      986
```

Two things fall straight out of that, and both matter:

- **Where the values actually are.** 982 of 986 samples were at or under 1 s.
- **Where measurement stops.** The highest finite bucket is 10,000 ms, because nothing configures the
  histogram and those are the OpenTelemetry SDK's default boundaries. Anything slower lands in `+Inf`, and
  `histogram_quantile` then reports the top finite bound — a percentile pinned at exactly `10000` that
  cannot move however slow things get. For a call that legitimately waits up to `DKQ_MAX_WAIT` (30 s), that
  is a number that looks real and is not.

Checking the boundaries is worth doing once per new metric, before trusting any percentile built on it.

### Percentiles, and the trap in them

```bash
curl -s 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=histogram_quantile(0.99, sum by (le, operation) (rate(dkq_operation_latency_milliseconds_bucket[10m])))'
```

Two things go wrong here often enough to name:

- **`NaN` usually means no traffic, not no data.** `rate()` over a counter that has stopped increasing is
  zero, and a quantile over zero rate is undefined. Query *while* the load is running, or widen the window
  until it covers the traffic. This is the single most common way to conclude "the metrics are broken" when
  they are fine.
- **`sum by (le, ...)` is mandatory**, and `le` must be in the grouping. Aggregating a histogram without it
  destroys exactly the dimension the quantile is computed over.

### Range queries, for a shape rather than a number

```bash
curl -s 'http://localhost:9090/api/v1/query_range' \
  --data-urlencode 'query=sum by (operation) (rate(dkq_operation_hits_total[1m]))' \
  --data-urlencode "start=$(date -v-15M +%s)" \
  --data-urlencode "end=$(date +%s)" \
  --data-urlencode 'step=15'
```

`/api/v1/query_range` is what a Grafana panel calls. Reaching for it directly is the quickest way to find
out whether a panel is empty because the query is wrong or because the window is.

### Is the target even being scraped

```bash
curl -s 'http://localhost:9090/api/v1/targets' | jq -r '.data.activeTargets[] | "\(.labels.job) \(.health) \(.lastError)"'
```

A scrape that is failing shows here and nowhere else. Worth checking before assuming an instrumentation
problem — in this stack the target is Jaeger's Prometheus exporter, not the service itself.

## Jaeger

Jaeger's own API, on the UI's port.

```bash
# who has reported anything — the trace equivalent of listing metric names
curl -s 'http://localhost:16686/api/services' | jq -r '.data[]'

# recent traces for a service
curl -s 'http://localhost:16686/api/traces?service=distributed-keyed-queue&limit=20' \
  | jq -r '.data[].spans[] | .operationName' | sort | uniq -c | sort -rn
```

That last one — counting span names — is the fastest answer to "what is actually instrumented", and it is
how you notice something *missing*. It is what showed that this service produces `EVALSHA` and `XREAD`
spans from the Java agent, and **no gRPC server spans**, because the agent does not recognise zio-grpc.

Useful filters: `&operation=<name>`, `&minDuration=1s` (find the slow ones), `&tags={"error":"true"}`.

## Grafana

Rarely worth curling — but two cases are:

```bash
# are the provisioned datasources actually there
curl -s -u admin:admin 'http://localhost:3000/api/datasources' | jq -r '.[] | "\(.name) \(.uid) \(.type)"'

# export a dashboard you arranged in the UI, to commit it
curl -s -u admin:admin 'http://localhost:3000/api/dashboards/uid/<uid>' | jq '.dashboard' > data/grafana/provisioning/dashboards/dkq.json
```

The second is the whole dashboard workflow: arrange panels in the browser, export, commit. Hand-authoring
Grafana JSON is not worth an afternoon.

## A short debugging order

When a panel is empty, work outwards — each step rules out a layer, and skipping to the middle is how an
hour disappears:

1. **Is the agent even on?** `OTEL_JAVAAGENT_ENABLED` must be `true`; the image defaults it off. Without it
   the SDK is a no-op and everything below is empty by design
   ([`java-agents-and-telemetry.md`](java-agents-and-telemetry.md)).
2. **Is anything being produced?** `/api/v1/label/__name__/values`, and `/api/services` for traces.
3. **Is the scrape healthy?** `/api/v1/targets`.
4. **Does the raw series have samples?** Query the metric bare, with no `rate` and no aggregation.
5. **Only then, the query.** `NaN` and empty results are usually the window or a missing `le`, not the data.

Steps 1–4 are all one `curl` each, and between them they locate the problem before anyone opens a
dashboard.
