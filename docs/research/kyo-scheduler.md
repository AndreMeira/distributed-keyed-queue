---
title: Does Kyo's adaptive scheduler beat ZIO's on the claim path?
type: research
status: current
updated: 2026-09-06
tags: [performance, zio, kyo-scheduler, benchmarking, throughput, measurement]
---

# Does Kyo's adaptive scheduler beat ZIO's on the claim path?

**No evidence either way, and the more useful finding is that this harness cannot answer the question.**

`kyo-scheduler-zio` replaces ZIO's executor with Kyo's adaptive scheduler. It is a one-line change —
`KyoSchedulerZIOAppDefault` does nothing but `override val runtime = KyoSchedulerZIORuntime.default` — which
makes it cheap to try and tempting to believe in.

## How it was measured

The dependency ships in the image either way and the scheduler is chosen by `DKQ_SCHEDULER=kyo`, so **one
build measures both arms** and nothing else can differ between them. Three rounds, alternating arms so that
drift over the session would hit both equally: one instance, agent off, `bin/demo.sh flood` (8 producers, 16
consumers, 512 keys, batch 8, 30s).

## What came back

| round | ZIO msg/s | Kyo msg/s | difference |
|---|---|---|---|
| 1 | 3893 | 3822 | −1.8% |
| 2 | 3791 | 3366 | −11.2% |
| 3 | 3407 | 4086 | **+19.9%** |
| median | 3791 | 3822 | +0.8% |

The paired differences run from −11% to +20%. **The spread between repetitions of the same configuration is
several times larger than any plausible effect**, so the +0.8% median difference is noise and nothing here
supports a claim in either direction.

Note what round 3 does to an interpretation that looked obvious after two rounds: both arms had declined
monotonically, which read convincingly as thermal drift — and then the last run was the fastest of all six.
It was variance the whole time. Two points make a line, and the line was wrong.

## Why the harness cannot answer this

- **The box is saturated across tenants.** During a flood the host sits at ~98% CPU with the server (~3.8
  cores), the demo client JVM, Redis and the observability stack all competing. Run-to-run scheduling of
  *containers* dominates scheduling of fibers.
- **The load generator is a co-tenant, and it kept ZIO's scheduler.** Only the server's runtime changed,
  which is the right scope for the question, but it means half the pipeline is unchanged and competing for
  the same cores.
- **The workload may not be scheduler-bound.** Most of a claim is a Redis round trip through Lettuce and
  Netty, plus GC. An adaptive scheduler wins where many fibers contend on fine-grained CPU work; there may
  simply be little of that here. [`../learning-material/reading-a-latency-tail.md`](../learning-material/reading-a-latency-tail.md)
  takes the same tail apart and finds allocation rate at the bottom of it, which is not something a
  scheduler can address.

## What would answer it

Ten or more rounds per arm, on a quiet machine, with the load generator on a *different* host, comparing
latency percentiles rather than a single throughput number. Until then the honest position is that this was
not measured, rather than that it made no difference.

## Status of the code

**This note is on `main`; the code it describes is not.** The `kyo-scheduler-zio` dependency and the
`DKQ_SCHEDULER=kyo` toggle live on the `kyo-scheduler` branch (`8304cd6`), deliberately unmerged — they cost
three jars in the image and one branch in `Main`, and they earned nothing measurable. The findings are kept
here so the experiment is not repeated from scratch; the branch is kept so it does not have to be rebuilt
from scratch either.

Merge it only when the experiment is re-run on cluster hardware, where the confounds above disappear. If
that never happens, delete the branch rather than merging it: an unused dependency that measured nothing is
not worth carrying.
