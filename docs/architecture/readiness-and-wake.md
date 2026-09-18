---
title: "How a consumer waits — readiness tokens, and the wake that feeds them"
type: architecture
status: current
updated: 2026-09-16
tags: [readiness, wake, tokens, consumers, blocking, queue, lock, architecture]
---

# How a consumer waits

A `dequeue` that finds nothing does not poll and does not hold a Redis connection open. It parks on an
in-process **readiness** until something says the queue may have work, then looks. This page is the
mechanism: what a readiness holds, why the queue's and the lock's differ, and what happens on every path
that could lose a wake.

What a waiter is *promised* is [`guarantees.md`](guarantees.md) (R4). What travels between instances is the
wake stream in [`redis-data-structures.md`](redis-data-structures.md). The lock's side of the same
machinery is in [`lock-mechanics.md`](lock-mechanics.md).

## Two readinesses, because the two questions differ

| | `QueueReadiness` | `LockReadiness` |
|---|---|---|
| holds | one token per queue, in a `Queue.sliding(1)` | one mailbox per waiting name |
| a wake reaches | **exactly one** parked consumer | **every** parked waiter on that name |
| why | any consumer can serve any key, so one look is enough | grants go by ticket order, so every waiter must re-ask whether it is the head |

Both are pure in-process coordination: they carry no work, and neither knows anything about Redis.

## One token, one consumer

The queue's readiness hands out **one** token per announcement. The alternative is a broadcast, which wakes
every consumer parked on that queue so that one of them can win the claim while the rest spend a round trip
to be told there is nothing for them — the thundering herd, at exactly the moment the store is busiest.

What replaces the broadcast is a **hand-on**: a consumer that looks and *finds* work offers the token
onward before it returns, so a burst of arrivals drains one consumer at a time. A consumer that looks and
finds nothing does not hand on, which is what stops the chain — otherwise consumers would spin on the store
for as long as they were willing to wait.

## A token is a hint that may be wrong, never a promise that may be lost

The asymmetry is deliberate, and everything else follows from it:

- a **spurious** token costs one wasted look, and cannot accumulate, because the buffer is `sliding(1)`
- a **lost** token costs a queue going quiet with work sitting in it, until some later announcement or the
  consumer's patience expires

So every path that could swallow a token puts one back — the patience elapsing, the effect failing, the
caller being interrupted — and those recoveries are **unconditional**. They have to be: a ZIO `Queue`
cannot report whether *this* taker received the element it gave up on, so "was there anything to put back?"
is not a question the code can answer. It offers regardless, and the buffer's size-1 sliding discards the
duplicate.

## Order does not matter, and that is a real simplification

Because the buffer remembers, a token offered while a consumer is mid-claim simply waits for that
consumer's next take. An earlier design used a promise-based signal, which had to be subscribed to *before*
looking or the wake was lost; that ordering constraint was load-bearing, untestable and easy to break. The
buffer removed it.

## A fresh queue starts with a token

A queue nobody has announced still gets one look. After a restart the wake stream is positioned at its end,
so work already sitting in `ready` will never be announced to this instance — without a seed token, the
first consumer would wait out its whole patience beside work that was already there.

## Where the wake comes from

`WakeConsumer` blocks on `XREAD` over its partition's wake stream and hands what accumulated to
`ReadinessSignalProcessor`, which routes each entry by the kind it carries: queue entries to `QueueReadiness`,
lock entries to `LockReadiness`. One stream feeds both, so a queue and a lock in the same partition share a
connection and a reader.

Losing a wake entirely is survivable and bounded: a consumer's patience expires, it returns empty, and the
caller asks again — the cost is latency, not work. That bound is R4 in [`guarantees.md`](guarantees.md).
