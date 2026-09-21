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

## 2. The client needs nothing added

This was the first finding, and it was a surprise. A batch consumer is already exactly right:

```scala
final case class Ready(id: String, cause: String)

producer <- Provider(client).producerWith[Ready]("conversations"): ready =>
              MessageId(ready.cause) -> MessageKey(ready.id)

batch    <- Provider(client).batched[Ready](Provider.BatchConsumerConfig("conversations", size = 32))
_        <- batch.consume(wakeAll).forever

def wakeAll(readies: List[Ready]): IO[MyError, Unit] =
  ZIO.foreachDiscard(readies.map(_.id).distinct)(work)
```

A claim is one key's messages, so every signal in a batch says the same thing. *"One claim, one outcome"*
— a constraint to design around for ordinary work — is precisely the semantic a signal wants, and the
collapsing is correct rather than convenient.

`Ready` carries the id because `batched[A]` hands logic values rather than envelopes, so the key does not
survive the decode. The id is addressing, not content.

An earlier draft of this proposed a `SignalProducer` / `SignalConsumer` pair in the client. It would have
added surface for nothing: the recipe above is the whole of it, and what is left is discipline rather than
API — signal after the state is written, settle a no-op rather than nacking it, and the id rule below.

## 3. The trap: not the key as the message id

The tempting move is `message_id == key`. One signal outstanding per key, later ones folded into it —
apparently sliding-one semantics for free, from a field the contract already describes as *"what makes a
repeated enqueue idempotent rather than a second copy"*.

It appears to lose wakes, and `enqueue.lua` says why in its own comment:

> *"The dedupe window is the key's current backlog, not all time: the id leaves this hash when the message
> is **settled**, so the same id may be sent again afterwards."*

Settled, not claimed. So for as long as a consumer is working a claim, an enqueue reusing that key as its
id should hit the `HSETNX` guard and be dropped silently — and a key whose backlog is empty at settle is
not returned to `ready`. A signal announcing something that happened *during* a claim would therefore
never be answered.

**The existing behaviour is right for what it was built for.** A producer retrying an at-least-once send
should not have the work done twice while it is being done. One `message_id` field is serving two intents,
and they only diverge while something is claimed.

The resolution keeps both: the id belongs to **the event that caused the signal**. The same event signalled
twice is one message, two events are two however busy the key is, and the idempotency the wire describes is
intact — what differs is the unit. For a signal the thing worth collapsing is the event, not the
destination.

Which also removes the reason to want per-message dedupe scoped to *queued* rather than *unsettled*, a
change floated in the companion note. Nothing here needs it.

## 4. The other trap: settle, never nack

A consumer that wakes, looks, and finds the state not ready for it should settle and stop. A nack carries
`retry_after`, which asks the **key** to wait — and whatever would make that state ready is usually another
signal on the same key, so deferring this one defers its own answer and the delay chosen races it. A
consumed signal costs one look; a wake that never arrives costs silence.

This one follows from the documented semantics of `retry_after` and needs no verification.

## 5. What would settle §3

One integration test, and it decides whether any of this is advice or a misreading:

> Enqueue a message for a key. Claim it. While the claim is held, enqueue again with the same key and the
> same `message_id`. Settle the claim. Assert whether the key returns to `ready` and the second message is
> delivered.

If it is dropped, §3 is a real trap and this page becomes usage guidance in
[`../learning-material/consuming-and-producing.md`](../learning-material/consuming-and-producing.md),
beside the batching section. If it is delivered, §3 is wrong and the `message_id == key` shape works after
all — which would be the simpler design, and worth knowing before anyone writes the longer one.
