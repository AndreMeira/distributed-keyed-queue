---
title: "The first end-to-end run, and the HTTP/2 corruption it found"
type: session
status: current
updated: 2026-08-23
tags: [e2e, netty, grpc, http2, debugging, dependencies]
---

# The first end-to-end run

The end-to-end suite (`e2e/`, described in
[`architecture/end-to-end-testing.md`](../architecture/end-to-end-testing.md)) was written to prove the
deployment properties: two instances are one queue, a killed pod loses no work, a key is never worked twice.
Its first run failed four of eight tests — and the reason turned out to be a bug that had been sitting in
`build.sbt` since the first commit.

Recording it because the *diagnosis* went wrong twice before it went right, and both wrong turns are the
kind that repeat.

## The symptoms

Three different errors, on tests that each passed in isolation:

```
INTERNAL: Encountered end-of-stream mid-frame                            (client, on dequeue)
INTERNAL: Connection closed after GOAWAY. HTTP/2 error code:
          COMPRESSION_ERROR, debug data: Incomplete header block fragment (client, on enqueue)
INTERNAL: http2 exception
  Suppressed: Http2Exception: Incomplete header block fragment
              at HpackDecoder.decode                                     (client, decoding a response)
```

Server-side, the matching complaint was `NettyServerStream deframeFailed: Encountered end-of-stream
mid-frame` — the server reading a **request** that ended mid-message.

## The two wrong turns

**"The containers are being killed."** They kept vanishing after a run, so the first theory was that the
harness was tearing down its own stack mid-suite. It was, but for an unrelated reason: `System.env` under
`ZIOSpecDefault` reads `TestSystem`, an empty map — so `DKQ_E2E_ENDPOINTS` was never seen and every run
composed its own stack and took it down at the end, including the one it had been told not to touch.
`TestAspect.ifEnvNotSet` reads the *live* environment, so the same variable was simultaneously set and unset
depending on who asked. Fixed by reading through `System.SystemLive`; the timeline of `docker events`
during a run is what settled it.

**"Docker Desktop's port forwarding is resetting connections."** Plausible — the errors all looked like a
connection dropped mid-frame, and macOS publishes container ports through a userland proxy. Killed by
running two instances directly on the host against the same Valkey: identical failures, no Docker in the
path.

## What it actually was

`build.sbt` pinned gRPC to one version, because an earlier grpc-netty/grpc-core split had failed loudly with
a `NoSuchMethodError`. Netty was left to resolve on its own, and resolved **split**:

```
netty-buffer / -common / -handler / -transport   4.1.118.Final   ← lettuce
netty-codec-http2 / -http / -socks               4.1.100.Final   ← grpc-netty 1.64
```

grpc-netty reaches into Netty's HTTP/2 internals, so an HTTP/2 codec running against another release's
internals corrupts HPACK header blocks — but only once several requests are in flight, which is why every
test passed alone and the whole thing had been invisible until a suite put concurrent traffic on one
connection.

**One bad write desynchronises the connection in both directions**, which is why the symptom kept changing
and why neither side's error named the culprit: the server blamed a truncated request, the client blamed a
corrupt response, and both were downstream of the same corrupted stream.

The decisive experiment was `WireSpec`: the same server driven twice, once through the **plain Java blocking
stub on plain threads** and once through the zio-grpc stub on fibers. The Java stub corrupted too — so
nothing about zio-grpc, ZIO fibers, or this repo's client code was involved, and the server was the one
writing garbage.

## The fix, and the rule

Align Netty **down** to what grpc-netty is built against (4.1.100), not up to the newest on the graph.
Lettuce uses stable Netty APIs and is happy there; grpc-netty is happy with nothing else. The overrides moved
to `ThisBuild`, so a second project cannot resolve its own versions and reintroduce it.

> **When bumping gRPC or Netty, bump them together.** Take the Netty version from the grpc-java release
> notes for the gRPC version in use, and set `nettyVersion` to exactly that. `WireSpec` fails if this slips.

## What this says about the suite

The bug was in the deployment's *dependency graph*, not in the queue. No amount of domain testing would have
found it, and the in-JVM `GrpcSpec` never will: it makes one call at a time. The e2e suite paid for itself on
its first run — which is also the argument for keeping `WireSpec` around now that it has nothing to prove.

Everything else the suite found was in the harness, not the service — leftover queue state between runs
(fixed with per-run queue names) and the `TestSystem` trap above. The queue itself passed every property it
was asked about, including a SIGKILLed instance losing no work.
