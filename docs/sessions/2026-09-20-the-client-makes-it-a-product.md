---
title: "Checkpoint: a client ships, and dkq becomes something another service can use"
type: session
status: current
updated: 2026-09-20
tags: [checkpoint, client, release, lock, queue, consumer, producer, milestone]
---

# The client makes it a product

Six pull requests took dkq from *a service with a wire* to *a thing another Scala service can depend on
and use correctly without reading the guarantees document first*. Released as **0.0.5**, the first release
carrying a client at all.

This records where the line now sits, and what is left — not the design reasoning, which lives with the
code and in the notes beside this one.

## What shipped

| | |
|---|---|
| [#30](https://github.com/AndreMeira/distributed-keyed-queue/pull/30) | the lock: `LockClient` (four RPCs) and `DistributedLock` (holds the lease around an effect) |
| [#31](https://github.com/AndreMeira/distributed-keyed-queue/pull/31) | publishing it, and a pom that stopped naming a registry consumers cannot read |
| [#32](https://github.com/AndreMeira/distributed-keyed-queue/pull/32) | the wire test nothing else covered: request shapes and status mapping over a real channel |
| [#33](https://github.com/AndreMeira/distributed-keyed-queue/pull/33) | `lease_ttl` on the queue's wire, and the queue's four RPCs in Scala types |
| [#34](https://github.com/AndreMeira/distributed-keyed-queue/pull/34) | the consumer that heartbeats and settles, the batch, the producer, and a page for each half |

72 client tests, 103 server, 17 end to end.

## The line the client draws

Layer 1 is the RPCs with better types and nothing withheld. Layer 2 takes on the three obligations the
service cannot enforce and the stubs do not mention:

- **the lease** — renewed while the caller's work runs, on a cadence taken from what the service granted
- **the settle** — on an answer, a failure *and* an interruption, so nothing taken is left owed
- **the payload** — a value in and a value out, with what will not decode handled by a stated policy

Everything layer 2 decides is reachable by dropping to layer 1. That rule held through the whole build and
is what settled most arguments: per-message outcomes, the fencing token, the batch size.

## Two things the wire owed the client

Both found by building against it rather than by reading it:

- **The lock's grant stated a deadline and not a span.** A managed holder timed its refreshes by the ttl it
  asked for, which the service silently clamps — so a hold could lapse before the first renewal.
- **The queue's claim had the same gap, worse:** its lease is the service's own configuration, so nothing
  the caller sent revealed it. Layer 2 was unbuildable without guessing.

The rule behind both, and the reason the batch ceiling needed no equivalent: *a service decision has to be
stated on the wire when the answer does not already reveal it.* A caller counts the deliveries it got;
nothing counts a lease.

## What a consumer has to know

Two resolvers, not one. The client depends on `homelab-common`, which lives in the toolkit's registry, and
published poms here carry no `<repositories>` — so a build listing only this repo's registry fails on a
transitive dependency with nothing to explain why. Both guides lead with it.

## Still open

Nothing blocking, in rough order of what would pay:

- **No end-to-end test drives the client.** It is covered by fakes and in-process channels; the deployed
  suite still talks to the generated stubs. A client that works against a real deployment is currently a
  belief rather than a result.
- **`sbt doc` runs only on a release**, so scaladoc breakage is invisible until one is cut — which is how
  three dead links from the package reorganisation reached 0.0.5's published docs
  ([#35](https://github.com/AndreMeira/distributed-keyed-queue/pull/35)). Adding `client/doc` to CI would
  move that to PR time.
- **The release workflow's Publish step** says it resolves nothing from the toolkit. That is no longer
  true — the client depends on it — and the step works because the Test step warms the coursier cache
  first. The fallback is load-bearing now rather than incidental.
- **The decoder's no-argument `derive` checks nothing**, not even the encoding, while `deriveAs` checks
  both. Unlike the payload type, the format is always derivable from the codec, so an unconditional
  encoding check may be the better default.
- Tests do not mirror the new `model`/`managed` subpackages, and the two payload typeclasses sit in
  `model` while being behaviour rather than data.
- Older, unchanged: duplicate CI runs awaiting branch protection, and `LockAcquireUseCaseSpec`'s sleeps
  standing in for "await parked".
