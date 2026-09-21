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
| [#39](https://github.com/AndreMeira/distributed-keyed-queue/pull/39) | the consumer delivering messages, with the typed one composed over it |

0.0.5 carries everything up to #34; #39 lands after it and goes out with the next release.

71 client tests, 103 server, 17 end to end.

## The line the client draws

Layer 1 is the RPCs with better types and nothing withheld. Layer 2 takes on the three obligations the
service cannot enforce and the stubs do not mention:

- **the lease** — renewed while the caller's work runs, on a cadence taken from what the service granted
- **the settle** — on an answer, a failure *and* an interruption, so nothing taken is left owed
- **the payload** — a value in and a value out, for the consumer that wants one

Everything layer 2 decides is reachable by dropping to layer 1. That rule held through the whole build and
is what settled most arguments: per-message outcomes, the fencing token, the batch size.

The third took a turn worth recording. The consumer first delivered values and kept an enum for the
messages that would not become one — retry it, drop it, drop it after so many tries. That enum was
`mapZIO` written out by hand, which the toolkit's NATS consumer had already noticed: it delivers messages
and lets a caller `map` them, so one policy about handler failures covers decoding too. dkq now does the
same, and `consumer[A]` is a consumer of messages plus a reading. Two things a fixed set of policies could
never express came with it — a dead letter, because that is a producer, and a queue carrying several
kinds, because `payloadType` is there to match on.

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

Nothing blocking, in rough order of what would pay. Four of what stood here shipped straight after:
[#40](https://github.com/AndreMeira/distributed-keyed-queue/pull/40) drives the client against a real
deployment, [#41](https://github.com/AndreMeira/distributed-keyed-queue/pull/41) puts `client/doc` in CI
and corrects the release workflow's comment, and
[#42](https://github.com/AndreMeira/distributed-keyed-queue/pull/42) mirrors the test tree and moves the
payload typeclasses out of `model`.

- **The decoder's no-argument `derive` checks nothing**, not even the encoding, while `deriveAs` checks
  both. Unlike the payload type, the format is always derivable from the codec, so an unconditional
  encoding check may be the better default. Reading being the caller's now makes this a smaller question
  than it was, not a settled one.
- Older, unchanged: duplicate CI runs awaiting branch protection, and `LockAcquireUseCaseSpec`'s sleeps
  standing in for "await parked".

## The beat follows the claims

The consumer's heartbeat used to be a fiber the provider forked at construction and a cadence the config
carried, which meant a consumer holding nothing still had a fiber asleep, and the first renewal of a fresh
claim waited out whatever interval that fiber had already started sleeping on — a 5-second default under a
5-second lease.

It now follows the toolkit `Batcher`: the state is `Idle` or `Beating`, the hold that finds the registry
empty forks the beat, and the beat stands down when the last claim is settled. Registering and forking
together refuse interruption, and the check that stands the beat down is the same atomic step that reads
the cadence, so a claim arriving alongside it either joins the running beat or starts one. The first sleep
is half the lease the service granted, because the claim states it before the fork.

The config's `heartbeat` knob went with it: there is no longer an interval that applies before a claim has
stated its own.
