---
title: "Signals — telling a consumer to look, without sending it anything"
type: learning-material
status: current
updated: 2026-09-22
tags: [client, signal, ready, consumer, producer, wake, coordination, zio]
---

# Signals

Sometimes the queue is not carrying work. One part of a service changes something a consumer owns — a row,
a document, a log — and the consumer has to notice. The change is already durable where it belongs, so the
message need not repeat it: it says *this key is worth looking at*, and the consumer reads the truth for
itself.

The client has a producer and a consumer for exactly that. Neither encodes nor decodes anything you wrote,
because a signal's key is the whole of what it says.

The queue half of the same client, for messages that do carry work, is
[`consuming-and-producing.md`](consuming-and-producing.md). Why this shape rather than a broadcast or a
poll is [`../research/the-queue-as-a-signal.md`](../research/the-queue-as-a-signal.md).

## Both halves

```scala
import homelab.keyedqueue.client.Endpoint
import homelab.keyedqueue.client.queue.{ Provider, QueueClient }
import homelab.keyedqueue.client.queue.model.Ready

ZIO.scoped:
  for
    client   <- QueueClient.scoped(Endpoint("dkq", 9000))
    signals  <- Provider(client).signalProducer("conversations")
    consumer <- Provider(client).signalConsumer(Provider.BatchConsumerConfig("conversations", size = 32))
    _        <- consumer.consume(look).forever
  yield ()

def look(ready: Ready): IO[MyError, Unit] = ???   // read the state of ready.id and act on it
```

`Ready(id)` is the key. Announcing one is `signals.emit(Ready(conversation))`, and the consumer is handed
the same value back.

## The order is always the same

```scala
for
  _ <- store.record(conversation, event)        // write the truth
  _ <- signals.emit(Ready(conversation))        // then say so
yield ()
```

Never the other way round. A consumer woken before the change has landed looks, finds nothing, and settles
— the announcement is spent on a state that had not changed yet.

Where the pair can be interrupted between the two — a request handler that dies has no second chance — put
them somewhere that is retried. A consumer under at-least-once delivery does both again, which costs one
duplicate signal, and duplicates are free.

## What the consumer is promised

**A signal is a hint that may be wrong, never a promise that may be lost.** Everything else follows:

- **A duplicate costs one read** of state the consumer was going to read anyway.
- **A stale signal is ordinary.** A key announced twice before anyone looked is looked at once, and what
  that look finds is whatever is true by then.
- **Order does not survive**, and nothing in a signal implies it did. A consumer learns that *something*
  happened, not how many times or in what sequence.
- **Losing one is the failure that matters**, and it is silent: the state sits changed with nothing coming
  to read it. So every path that could swallow a signal puts one back — unconditionally, without reasoning
  about whether it was needed.

`size` on the config decides how many announcements one claim collapses. A claim is one key's messages, so
they all name that key, and the consumer folds them to the one `Ready` they agree on and runs the logic
once. Anything past `size` stays queued, the key returns to `ready`, and the next claim finds nothing left
to do and settles.

## Settle a signal you cannot act on; never nack it

A consumer that looks and finds the state not ready for it — a half-finished turn, a dependency still
running — returns normally. The claim settles, the signal is spent, and whatever will make that state ready
sends its own signal when it does.

Failing the logic instead nacks the message, and a nack asks the **key** to wait for `retryAfter`. Since
what would unblock this consumer is usually another signal on that same key, the wait delays its own answer
and the duration is racing it. Reserve failure for what it means everywhere else in this client: the work
was owed and did not happen.

## What it costs

Each announcement is a small message that a claim has to carry. The number of them is bounded by how many
things actually changed — no timer produces any — so a quiet key costs nothing at all and a busy one costs
one claim per batch. A consumer that reads expensive state on every signal pays that read per *event*,
which is the trade: a queue carrying the change itself would pay less per wake and need the two to agree
about what a change is.

## What is not covered by a test yet

The client's unit tests cover the producer and the fold. Nothing exercises signals against a running
service, so the e2e suite has no case for them, and the interaction between a repeated `message_id` and a
key that is currently claimed — the reason a signal mints a fresh id rather than reusing its key — is
reasoning from `enqueue.lua` rather than an observation. The research note states the test that would
settle it.
