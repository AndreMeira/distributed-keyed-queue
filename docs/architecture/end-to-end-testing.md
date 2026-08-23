---
title: "The end-to-end suite — a real deployment, driven over the wire"
type: architecture
status: current
updated: 2026-08-23
tags: [testing, e2e, docker-compose, grpc, distributed, load]
---

# The end-to-end suite

Two dkq instances, one Valkey, clients over TCP. Everything in `e2e/` exists to test the properties that
**only exist in a deployment** — the ones a single-process test cannot fail.

```
sbt e2e            # build the image, compose the stack, run the suite, tear it down
sbt e2e/test       # run against a stack that is already up (skips the image build)
```

## Why it is a separate project

`sbt test` must stay something worth running on every save. The suite in `src/test/` runs the service in the
test's own JVM against a Testcontainers Valkey and takes seconds; this one builds a Docker image, composes a
stack and takes a minute or two. Different cadence, different project — `e2e/` depends on the root project,
so its clients are the same generated stubs a real consumer would use.

## What it proves that `GrpcSpec` cannot

`GrpcSpec` proves the API behaves. These are the claims that need more than one process to be false:

| Test | The claim |
|---|---|
| round trip across instances | two instances are **one queue**, not two |
| **cross-instance wake** | a `Dequeue` parked on A is woken by an `Enqueue` on B — phase 1's exit criterion |
| order across instances | a key stays FIFO though four consumers on two instances compete for it |
| one key, one worker | the invariant the whole design exists for, under real concurrency |
| **instance killed mid-handler** | a SIGKILLed pod's claim lapses and its work comes back, `attempt = 2` |
| heartbeat past the lease | a 15s handler keeps a 5s lease alive by beating |
| silence loses the claim | and the late settle is **refused**, so no message is completed twice |
| load | 120 messages, 8 keys, 8 consumers: nothing lost, nothing doubled, every key ordered |
| `WireSpec` | one channel survives concurrent calls — see [the Netty pin](#the-netty-pin) |

**No load balancer in front of the instances, deliberately.** The client addresses `dkq-a` and `dkq-b`
directly. Behind a proxy you could not tell which instance served a call, and the cross-instance tests would
prove nothing.

## Running it against something else

`DKQ_E2E_ENDPOINTS=host:port,host:port` points the same suite at a deployment it does not own — the cluster, a
colleague's laptop — and nothing is started or stopped. The kill test skips itself there, because killing
somebody else's pod is not this suite's business.

Two properties make that safe, and both were bugs first:

- **Every queue name is suffixed with a token unique to the run** (`Deployment.queue`). A shared deployment
  cannot be wiped between runs, and leftovers arrive as extra messages with `attempt` already counted —
  failing tests for reasons that have nothing to do with the code.
- **The environment is read live** (`Deployment.live`), not through `System.env`, which under `ZIOSpecDefault`
  reads `TestSystem` — an empty map. That silently composed a stack, and then tore down the deployment it had
  been told not to touch.

`DKQ_E2E_KEEP=1` leaves the stack up afterwards, for when a failure needs its logs and its Redis.

## Timing, and why these tests are not flaky by accident

Every wait is real (`TestAspect.withLiveClock`); a virtual clock would prove nothing about a deployment. The
margins are chosen so a slow machine does not fail them:

- The instances run a **5s lease and a 2s sweep** (`docker-compose.e2e.yml`), so recovery is a wait of about
  eight seconds rather than the thirty-something a production lease implies.
- The blocking test asserts `elapsed >= 1900ms` against a 2s delay and `<= 10s` against a 20s patience — wide
  enough for a stall, tight enough that "woken" and "waited it out" stay distinguishable.
- The exclusivity test measures each hold **from after the claim returned to before the settle was sent** —
  an interval strictly inside the one the server considers the key held. An overlap is therefore a real
  overlap, never a round trip masquerading as one.
- Per-key order is recorded **before** the settle is sent, because a key becomes claimable again only once
  the settle has applied. Recording afterwards would let two consumers race to append.

## The load test is not a benchmark

It asserts correctness under concurrent pressure and prints throughput as *information*. There is no rate
control and there are no percentiles, and the number it prints is dominated by per-key serialisation — eight
keys means at most eight messages in flight, by design. For real numbers point `ghz` at the same stack.

## The Netty pin

The suite's first run found a bug that had been in the build from the start: **lettuce pulls Netty 4.1.118,
grpc-netty 1.64 is built against 4.1.100**, and mixing them makes the *server* emit corrupt HPACK header
blocks once several requests are in flight. The client reports `Incomplete header block fragment`, the server
reports a truncated request, and neither is the culprit — one bad write desynchronises the connection in both
directions.

It reproduces through the plain Java blocking stub, so it is nothing to do with zio-grpc, and it cannot
appear in a single-threaded test. `build.sbt` now pins Netty to what grpc-netty was built against, and
`WireSpec` is the minimal reproduction kept as a regression guard. **When bumping either, bump them
together**: take the Netty version from the grpc-java release notes.

What building it taught — the ZIO Test traps, how to read an HTTP/2 failure, why the Netty pin goes *down* —
is in [`../learning-material/writing-end-to-end-tests.md`](../learning-material/writing-end-to-end-tests.md).

## Known limits

- **Two instances, fixed.** Three would test ownership handoff better, but nothing in phase 1 is
  ownership-aware, so it would cost minutes per run to prove nothing new.
- **A consumer stops on its first empty reply**, so a test asserts on the total rather than on which consumer
  got what. With generous patience this is only a risk if a key is held longer than the patience.
- **Not wired into CI.** It needs a Docker daemon and an image build for a suite whose value is in being run
  deliberately — before a release, after touching the substrate or the wire.
