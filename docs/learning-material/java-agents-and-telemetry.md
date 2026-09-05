---
title: "Java agents: what they do to your build, and how to keep them out of dev"
type: learning-material
status: current
updated: 2026-09-05
tags: [opentelemetry, java-agent, sbt, native-packager, docker, observability]
---

# Java agents, and where they belong

A Java agent is not a library. Nothing imports it, nothing calls it, and putting it on the classpath is a
mistake rather than a shortcut. That one fact decides almost everything about how it enters a build, so it
is worth being precise before touching `build.sbt`.

This note is what dkq's telemetry setup does and why. Everything in it was measured on this repo; the
numbers are from a laptop and are there for the ratios, not the absolutes.

## What an agent actually is

The JVM accepts a flag:

```
java -javaagent:/path/to/opentelemetry-javaagent.jar -jar yourapp.jar
```

Before `main` runs, the JVM hands that jar a chance to **rewrite the bytecode of classes as they load**. The
OpenTelemetry agent uses it to recognise libraries — gRPC, JDBC, Lettuce, Kafka — and wrap their entry
points so each call opens a span and records a metric. Your code is untouched and unaware. This is why the
agent can instrument a library you never configured, and why it can do nothing at all if it is merely on
the classpath: no `-javaagent` flag, no rewriting.

Two consequences follow, and they are the whole design problem:

1. **It is a run-time decision, not a compile-time one.** The same artifact is instrumented or not
   depending on how the JVM was launched.
2. **It is all-or-nothing per process.** There is no "instrument this part". The agent either loads and
   rewrites, or does not.

## How it enters an sbt build

The instinct is `libraryDependencies`. Resist it: that puts the agent on the application classpath, where
it does nothing useful and risks clashing with the versions of the libraries it also contains.

There is a plugin for this, and it exists precisely because the mechanics are fiddly:

```scala
// project/plugins.sbt
addSbtPlugin("com.github.sbt" % "sbt-javaagent" % "0.1.8")
```

```scala
// build.sbt
lazy val server = project
  .enablePlugins(JavaAppPackaging, DockerPlugin, JavaAgent)
  .settings(
    javaAgents += "io.opentelemetry.javaagent" % "opentelemetry-javaagent" % otelAgentVersion % "dist",
  )
```

**The scope on that line is the whole trick.** `sbt-javaagent` accepts several, and they answer different
questions:

| scope | agent attached when… | use it for |
|---|---|---|
| `compile` | `sbt run` | you want telemetry in local development |
| `test` | `sbt test` | instrumenting the test JVM, rarely what you want |
| `dist` | the **packaged** application runs | production, and nothing else |

dkq uses `dist` alone. That is the answer to "agent in prod but not in dev", and it is a single word rather
than a run mode, an environment variable or an `if` in `Main`: **`sbt run` has no agent to enable, because
the agent is not there.**

### What the plugin does for you

Run `server/Docker/stage` and look at the generated start script:

```bash
addJava "-javaagent:${app_home}/../opentelemetry-javaagent/opentelemetry-javaagent-2.20.1.jar"
```

The plugin resolved the jar, placed it in the distribution, and **wrote the flag into the launcher**. A
deployment therefore never has to know the path, the version, or that a `-javaagent` flag exists. Compare
that with the hand-rolled alternative — a custom Ivy configuration plus a `Universal / mappings` entry —
which puts the jar in the image but leaves the flag as something an operator has to get right in a
`JAVA_OPTS` somewhere. Both work; only one of them cannot be got wrong later.

### What it does *not* do

The jar is real weight: **23 MB**, in the image whether or not it is ever used, and there is no way to
scope it to "only when deployed to production" because the image *is* the artifact. If that matters more
than the convenience, the alternative is to not ship it at all and let the Kubernetes OpenTelemetry
Operator inject it at pod admission — same `-javaagent` flag at the end, decided by the cluster rather than
by the build. That trades 23 MB for an operator and its CRDs.

## The part that bites: an agent that loads has opinions

Scoping to `dist` means the agent is attached **every time the image runs** — including the end-to-end
suite, a local `docker run`, and anything else that is not production. With no collector to talk to, this
is what dkq's container printed:

```
ERROR io.opentelemetry.exporter.internal.http.HttpExporter - Failed to export metrics.
java.net.ConnectException: Failed to connect to localhost/[0:0:0:0:0:0:0:1]:4318
        at okhttp3.internal.connection.ConnectPlan.connectSocket(ConnectPlan.kt:282)
        ...
```

on a loop, with stack traces, forever. The agent defaults to exporting OTLP to `localhost:4318`, and an
absent collector is not a reason for it to stop trying.

So a second switch is needed, and there are two candidates. Measured on the same image:

| setting | export failures | startup |
|---|---|---|
| `OTEL_JAVAAGENT_ENABLED=false` | 0 | **1048 ms** |
| `OTEL_TRACES_EXPORTER=none` (and metrics, logs) | 0 | 2499 ms |

Both are silent; only one is cheap. Turning the *exporters* off still leaves the agent rewriting every
class it recognises — about **1.4 seconds of startup** for instrumentation whose output is discarded.
`OTEL_JAVAAGENT_ENABLED=false` skips the rewriting entirely, and the agent's only trace is one version
banner.

That is dkq's image default:

```scala
dockerEnvVars := Map("OTEL_JAVAAGENT_ENABLED" -> "false"),
```

which lands in the Dockerfile as `ENV OTEL_JAVAAGENT_ENABLED="false"`. The agent travels, dormant, and one
variable wakes it.

## So: three states, and what selects each

| where | agent present? | agent active? | selected by |
|---|---|---|---|
| `sbt run`, `sbt test` | no | — | the `dist` scope |
| the image, by default (e2e, a plain `docker run`) | yes | no | `ENV OTEL_JAVAAGENT_ENABLED=false` |
| the image, deployed | yes | yes | the deployment sets it `true` + an endpoint |

Note what is *not* in that table: no run mode, no profile in the application, no branch in `Main`, no
config key dkq reads. The application code is identical in all three, which is the property worth
protecting — a telemetry setup that changes what the program does has stopped being observability.

## Seeing it locally

Because the agent only ships in the image, `sbt run` can never show you a trace. That is a deliberate
trade, and it means local observability needs the container:

```bash
bin/run.sh          # stack up, image built, service running with the agent
bin/run.sh down     # stop both
```

which is the three steps that actually matter, in order:

```bash
docker compose --profile telemetry up -d   # Jaeger, Prometheus, Grafana
sbt server/Docker/publishLocal             # build the image
bin/run-with-telemetry.sh                  # run it with the agent on
```

The observability services sit behind a **compose profile**, so `docker compose up` is still Valkey alone
and costs a developer who is not looking at telemetry exactly nothing.

`bin/run-with-telemetry.sh` — the last of those, and what `bin/run.sh` ends by exec'ing — is where
the three "on" variables live:

```
OTEL_JAVAAGENT_ENABLED=true
OTEL_SERVICE_NAME=distributed-keyed-queue
OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318
```

**Jaeger v2 is the collector.** It is built on the OpenTelemetry Collector, so it receives OTLP directly —
no separate collector container — stores traces in memory, serves the UI on 16686, and re-exports metrics
on 8889 for Prometheus to scrape. Both signals leave the application over one connection and split there.

Two OTLP ports, and which one you use is a choice, not a fallback: **4317 is gRPC, 4318 is HTTP**. The
agent defaults to HTTP, hence `4318` above; `OTEL_EXPORTER_OTLP_PROTOCOL=grpc` plus `4317` is the other
door into the same collector.

## What you get for free, and what you do not

After one `Enqueue` through the running container, Jaeger had:

```
   17  XREAD
    3  EVALSHA
```

Those are **Lettuce spans**. Nobody wrote them; the agent recognised the Redis client and instrumented it.
Prometheus had 19 `dkq_*` series (JVM heap, GC, CPU, class loading) on the same terms.

And now the honest part. There are **no gRPC server spans in that list.** The agent instruments
`io.grpc`'s server, but dkq serves through zio-grpc, whose interceptor chain the agent does not recognise —
so a request produces Redis spans with no parent, and a trace that starts in the middle of the story.

This is the general shape of agent instrumentation, and the thing to expect rather than be surprised by:
**an agent covers the libraries it knows, and knows nothing about your domain.** No span says
"DequeueUseCase", because no library call corresponds to one. Manual instrumentation — dkq's `Monitor`
port, wrapping use cases — is what supplies the root those library spans hang under, and what names
operations in your vocabulary rather than Redis's.

The two compose rather than compete: when the agent is loaded, `OpenTelemetry.global` returns *its* SDK, so
manually created spans join the same trace automatically. The one thing to get right is context storage —
zio-telemetry's default keeps the current span in a `FiberRef`, while the agent uses a thread-local, so
`OpenTelemetry.contextJVM` is what makes manual spans nest under agent spans instead of forming a second,
disconnected tree.

## If you remember three things

1. **An agent is a launch flag, not a dependency.** Its scope in the build decides which environments carry
   it; `dist` means "packaged only", which is usually what "prod, not dev" means.
2. **Present is not the same as active.** Default it off in the image, or a run with no collector will fill
   your logs and cost 1.4 s of startup for nothing.
3. **The agent gets you the libraries; you get you the domain.** Free spans for Redis and JDBC are worth
   having, and they will not tell you which use case was slow.
