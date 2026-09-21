---
title: "Signals in the client, and a flaky heartbeat test to watch"
type: session
status: current
updated: 2026-09-22
tags: [session, signal, ready, client, provider, partition, flaky-test, ci]
---

# Signals in the client

Came out of designing an agent loop in the ZIO toolkit, where the queue carries no work at all: a message
says *this conversation is worth looking at*, and the consumer reads the transcript for itself. That use
turned out to need nothing new from the service and very little from the client — and what it did need,
[#46](https://github.com/AndreMeira/distributed-keyed-queue/pull/46) added.

## What landed

| | |
|---|---|
| `Ready(id)` | a signal: the key worth looking at, and nothing else |
| `Provider.signalProducer(queue)` | emits one, with a fresh message id per call |
| `Provider.signalConsumer(config)` | folds a claim's announcements to the distinct keys they name |
| `research/the-queue-as-a-signal.md` | why, and the one claim in it that is still unverified |
| `learning-material/signal-synchronisation.md` | how, including what a signal is and is not promised |

Two things fell out that were worth more than the code.

**The client needed no new concept.** A signal producer is an adapter for `homelab.common.messaging.Producer`,
not an abstraction of its own, and the consuming half is a batch consumer — because a claim is one key's
messages, so every signal in a batch says the same thing and *"one claim, one outcome"* stops being a
constraint to design around and becomes the semantic. An earlier draft proposed a `SignalProducer` /
`SignalConsumer` pair; it would have been surface for nothing.

**Neither half encodes or decodes anything.** The producer writes the key into the envelope and the consumer
reads it back off, so there is no payload and nothing for the two halves to disagree about. That removed a
coupling the first version had: an encoder using `derive` writes `unnamed` as its payload type, and a
consumer using `deriveAs` would have refused everything it sent, invisibly, because the encoder sits in a
private companion.

`ManagedProducer` also stopped holding a function: it takes a `Partition` and asks it two named questions,
which made `producer` the primitive on `Provider` and `producerWith` the convenience over it.

## Keep an eye on: a flaky HeartbeatSpec

**`Heartbeat / "a claim taken as the last one is settled joins the beat rather than starting a second"`**
failed on CI during #46 and passed on the duplicate run *of the same commit*. It has nothing to do with
what that PR changed.

```
✗ Result was false
first.isDefined
first = None
HeartbeatSpec.scala:74
```

The suite runs `@@ TestAspect.withLiveClock`, and the assertion is on `client.beats.take.timeout(soon)` —
a fixed window for the first beat to arrive. A contended runner misses it and `first` is `None`. The test's
own comment concedes the ceiling it checks is "a ceiling a slow runner only falls under", which is the
second half of the same problem: it is measuring a real-time cadence on shared hardware.

Worth watching rather than fixing blind, because the thing it pins is real — the claim taken as the last
one is settled must join the running beat instead of electing a second, which is the drain-fiber election
and has shipped wrong here once before. If it fails again:

- **check whether `first` or `sent` is the failing assertion.** `first = None` is the timing window and
  says nothing about the election. A `sent` above the ceiling is the election, and that is a real bug.
- the durable fix is a `TestClock`, which means the beat's cadence has to be observable rather than slept
  through. That is a change to `Heartbeat`, not to the spec, and it is why this is logged rather than done.

## Also worth knowing

CI runs three things (`.github/workflows/tests.yml`), and the third is easy to miss: `sbt test`,
`sbt e2e/Test/compile`, and `sbt client/doc` grepped for `Couldn't resolve a member for the given link
query`. A single unresolved scaladoc link fails the build. **Scaladoc does not follow imports** — a
`[[Ready]]` written in `queue/managed/` does not resolve, because `Ready` is in the sibling `queue.model`
package, while `[[QueueClient]]` and `[[ServiceError]]` do through package nesting. Qualify across
packages: `[[model.Ready]]`.

## Still open

- **Nothing exercises signals against a running service.** The e2e suite has no case for them.
- **The reason a signal mints a fresh message id is reasoning, not observation.** `enqueue.lua` dedupes
  until a message is *settled*, which covers the whole time a consumer works it — so reusing the key as the
  id should drop announcements made during a claim. The test that would confirm it is in
  [`../research/the-queue-as-a-signal.md`](../research/the-queue-as-a-signal.md) §5, and it is cheap.
- **`Ready`'s payload is dead weight.** The producer serialises the case class and the consumer never opens
  it. Either shrink it to empty bytes or keep it as something legible to whoever inspects the queue, but it
  is currently paying for a round trip nobody reads.
