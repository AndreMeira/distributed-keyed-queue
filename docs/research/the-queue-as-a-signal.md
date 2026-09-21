---
title: "The queue as a signal — what it would take, and a trap in the way"
type: research
status: draft
updated: 2026-09-21
tags: [signal, wake, consumer, batch, message-id, dedupe, exploration, not-a-decision]
---

# The queue as a signal

> **Exploratory, and unverified.** Nobody has run this against dkq. The trap in §3 is read off
> `enqueue.lua` and the wire contract rather than observed, and the test that would settle it is named at
> the end. Nothing here is a recommendation yet.

Sketched in the client since this was written — `ManagedSignalProducer`, and `Provider.signalProducer` —
so §2 describes code rather than a proposal. Everything about §3 remains a reading.

Came out of the same session as
[`two-verbs-an-agent-consumer-asked-for.md`](./two-verbs-an-agent-consumer-asked-for.md), designing an
agent loop whose queue messages carry nothing at all. The pattern's rationale — why a signal carries no
information, and what that buys — is a note in the toolkit,
`homelab-toolkit-zio/docs/research/a-one-element-signal.md`. This one is only about whether dkq serves it,
and what a caller would write.

## 1. The use

Sometimes the queue is not carrying work. It is saying that something a consumer owns has changed, while
the change itself is already durable elsewhere — a row, a document, a log. The message carries no
information the consumer needs: it says *look at this again*, and the consumer reads the truth for itself.

The appeal is that every delivery property then stops mattering except one. Duplicates are free, late is
fine, order is irrelevant; only losing a signal is fatal, because the loss is silent — work sits with
nothing to notice it, and nothing fails while that is true.

## 2. No new concept, just an adapter

The first draft of this proposed a `SignalProducer` / `SignalConsumer` pair in the client. That was surface
for nothing: a signal producer is an adapter for a port the toolkit already has.

```scala
final case class Ready(id: String)

class ManagedSignalProducer(client: QueueClient, queue: String) extends Producer[ServiceError, Ready]:
  override def emit(value: Ready): IO[ServiceError, Unit] =
    for
      id     <- Random.nextUUID.map(_.toString)
      message = MessageEncoder.message(MessageKey(value.id), MessageId(id), value)
      _      <- client.enqueue(queue, message)
    yield ()
```

`homelab.common.messaging.Producer` is the port; this is one more implementation of it, reached through
`Provider.signalProducer(name)` beside `producer` and `producerWith`. A caller that already emits to a
producer emits signals the same way, and the in-memory implementation that exists for every other port
serves this one too.

The consuming half needs nothing at all. `Provider.batched[Ready]` is already right, because a claim is one
key's messages — so every signal in a batch says the same thing, and *"one claim, one outcome"*, a
constraint to design around for ordinary work, is precisely the semantic a signal wants:

```scala
def wakeAll(readies: List[Ready]): IO[MyError, Unit] =
  ZIO.foreachDiscard(readies.map(_.id).distinct)(work)
```

`Ready` carries the id because `batched[A]` hands logic values rather than envelopes, so the key does not
survive the decode. The id is addressing, not content.

## 3. The id is fresh, and that inverts the usual rule

`producerWith`'s doc says the id comes from the value, because "an id derived from what is being sent makes
a retried emit the same message rather than a second one". A signal wants the opposite, and the emit above
mints a UUID per call.

The reason is that the two are protecting against different things. Deduplicating a retried *message* keeps
work from being done twice, which matters. Deduplicating a repeated *signal* would suppress an
announcement — and a consumer that has already looked cannot know it needs to look again. A duplicate
signal costs one read of state the consumer was going to read anyway. The asymmetry the whole pattern rests
on says which way to take that trade.

### What `message_id == key` would have bought, and appears not to

The tempting shape is the id *being* the key: one signal outstanding per conversation, later ones folded
into it — sliding-one semantics from a field the contract already describes as "what makes a repeated
enqueue idempotent rather than a second copy".

It appears to lose wakes, and `enqueue.lua` says why in its own comment:

> *"The dedupe window is the key's current backlog, not all time: the id leaves this hash when the message
> is **settled**, so the same id may be sent again afterwards."*

Settled, not claimed. For as long as a consumer is working a claim, an enqueue reusing that key as its id
should hit the `HSETNX` guard and be dropped — and a key whose backlog is empty at settle is not returned
to `ready`. A signal announcing something that happened *during* a claim would never be answered.

The existing behaviour is right for what it was built for: a producer retrying an at-least-once send should
not have the work done twice while it is being done. One `message_id` field serving two intents, diverging
only while something is claimed. A fresh id per signal sidesteps it entirely, at the cost of one stored
entry per announcement — which the batch claim collapses at the point it matters.

This also removes the reason to want per-message dedupe scoped to *queued* rather than *unsettled*, floated
in the companion note. Nothing here needs it.

## 4. The other trap: settle, never nack

A consumer that wakes, looks, and finds the state not ready for it should settle and stop. A nack carries
`retry_after`, which asks the **key** to wait — and whatever would make that state ready is usually another
signal on the same key, so deferring this one defers its own answer and the delay chosen races it. A
consumed signal costs one look; a wake that never arrives costs silence.

This one follows from the documented semantics of `retry_after` and needs no verification.

## 5. What would confirm §3

The fresh id means nothing here depends on the reading being right. It is still worth one integration test,
because the next person will think of `message_id == key` too — both of us did — and the note should say
whether it was avoided for a real reason:

> Enqueue a message for a key. Claim it. While the claim is held, enqueue again with the same key and the
> same `message_id`. Settle the claim. Assert whether the key returns to `ready` and the second message is
> delivered.

If it is dropped, the trap is real, and the recipe belongs in
[`../learning-material/consuming-and-producing.md`](../learning-material/consuming-and-producing.md) beside
the batching section once something has run it. If it is delivered, §3 is wrong: `message_id == key` works,
the UUID is unnecessary, and a signal collapses in the queue rather than in the claim.
