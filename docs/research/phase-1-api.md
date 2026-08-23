---
title: "Phase 1 — a synchronous API for the keyed queue"
type: research
status: draft
updated: 2026-08-23
tags: [api, grpc, proto, phase-1, dequeue, lease, exploration]
---

# Phase 1 — a synchronous API

> **Exploration, and deliberately the smaller of two designs.** Phase 2 replaces the blocking `Dequeue` with
> a stream and credits ([`roadmap.md`](./roadmap.md)), which will change most of what is below. This is
> written to be *replaceable*: four unary calls, no streaming, no batching, and nothing a client has to learn
> that a stream will not also need.

Substrate design: [`redis-keyed-queue.md`](./redis-keyed-queue.md). Requirement:
[`research/infrastructure/homelab-message-broker.md`](../../../research/infrastructure/homelab-message-broker.md).

## The shape

Four calls, and a client loop that is a `while` with three statements in it.

```proto
service KeyedQueue {
  // Accept a message for a key. Returns once it is durably queued.
  rpc Enqueue(EnqueueRequest) returns (EnqueueResponse);

  // Block until work for this queue is available, or `wait` elapses.
  rpc Dequeue(DequeueRequest) returns (DequeueResponse);

  // Report the outcome. Until this lands, the message is still leased to you.
  rpc Settle(SettleRequest) returns (SettleResponse);

  // Renew every claim this consumer still holds. Sent on a fixed tick, not per message.
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
}
```

`Settle` rather than separate `Ack`/`Nack`: one call with an outcome keeps the client's happy path and its
failure path structurally identical, and it borrows the vocabulary the toolkit already uses (a *verdict* is
filed, whichever way it went).

## Messages

```proto
message EnqueueRequest {
  string   queue    = 1;   // the address, not part of the message — see redis-keyed-queue.md §3b
  Envelope envelope = 2;
}
message EnqueueResponse {
  uint64 key_depth = 1;    // how many messages this key now has queued; a metric, not a decision
}

message DequeueRequest {
  string                   queue    = 1;
  google.protobuf.Duration max_wait = 2;   // how long to block; the RPC deadline should exceed it
                                           // (not `wait`: that collides with Object.wait() on the JVM)
}
message DequeueResponse {
  optional Delivery delivery = 1;       // absent = nothing became ready in time. NOT an error
}

message Delivery {
  string   receipt = 1;                 // opaque; hand it back to Settle and Heartbeat
  Envelope envelope = 2;
  uint32   attempt  = 3;                // 1 on first delivery, 2+ after a reclaim
  google.protobuf.Timestamp lease_expires_at = 4;
}

message SettleRequest {
  string                   receipt     = 1;
  Outcome                  outcome     = 2;
  google.protobuf.Duration retry_after = 3;   // OUTCOME_FAILED only; zero means immediately
}
enum Outcome {
  OUTCOME_UNSPECIFIED = 0;
  OUTCOME_DONE        = 1;
  OUTCOME_FAILED      = 2;
}
message SettleResponse {
  Applied applied = 1;
}
enum Applied {
  APPLIED_UNSPECIFIED = 0;
  APPLIED_OK          = 1;
  APPLIED_STALE       = 2;   // your lease was revoked while you worked; the outcome was discarded
}

message HeartbeatRequest {
  repeated string receipts = 1;   // everything this consumer currently holds; empty is legal
}
message HeartbeatResponse {
  repeated string           stale         = 1;   // of those, the ones it no longer holds
  google.protobuf.Timestamp renewed_until = 2;   // the new deadline for all the rest
}
```

**One call renews everything, and the answer is what you lost.** A consumer sends the receipts it is holding
on a fixed tick; the server pushes every live claim's deadline forward and returns the subset that are
`stale` — claims revoked while the consumer was working them. Naming the losers rather than counting the
winners is the difference between an actionable answer and a metric: on receiving a receipt in `stale`, the
consumer must **stop working that message and discard its result**, exactly as for `APPLIED_STALE` from
`Settle`. An empty `receipts` list is legal and useful: it says "still here, holding nothing".

**The cadence is deployment-wide configuration, not an API parameter.** One lease TTL and one heartbeat
interval for every queue, set by environment (`DKQ_LEASE_TTL`, `DKQ_HEARTBEAT_INTERVAL`), which is why
neither appears in the request. That is a phase 1 simplification and a deliberate one: per-queue lease tuning
is a knob nobody can set correctly before there is traffic to observe. `renewed_until` still comes back on
every beat, so a consumer can pace itself from the server's clock rather than its own — the same skew
argument that applies inside the scripts — and the usual rule holds: beat at a fraction of the TTL, so one
lost round trip does not cost every claim at once.

**Do not confuse it with dkq's internal heartbeat**, which shares the name and nothing else. This one is the
consumer renewing **its claims**, one entry per held key. The internal one renews a **connection's** worker
liveness inside dkq and is invisible to clients. Different lifetimes, different failure domains — see
[`redis-keyed-queue.md`](./redis-keyed-queue.md).

## The three decisions worth arguing about

**A timeout is an empty response, not an error.** `DequeueResponse.delivery` is absent when nothing became
ready within `max_wait`. Returning `DEADLINE_EXCEEDED` for an *expected* outcome would push every client into
error handling for the normal quiet case, and would make a real deadline indistinguishable from an idle
queue. The client's own RPC deadline should be set slightly above `max_wait`, so the server answers first.

**`APPLIED_STALE` is a result, not a status code.** A settle whose lease was revoked mid-handler is an
expected outcome of an at-least-once system, not a fault — so it belongs in the response, where the client is
forced to branch on it, rather than in a `Status` it can accidentally treat as a retryable transport error.
The obligation it carries: **discard the work's result, do not retry, and do not touch that key** — somebody
else may hold it now.

**The receipt is opaque.** It encodes queue, key and the fence token, but the client is told none of that: it
receives a string and hands it back. That keeps the fencing scheme — the part most likely to change — behind
the API, and it stops clients inventing their own key arithmetic. Nothing is signed: forging a receipt buys
nothing, because the server validates the token against `fence` anyway.

## What the client does

```scala
def consumer(client: KeyedQueue, queue: String)(handle: Envelope => Task[Unit]): RIO[Scope, Nothing] =
  for
    held <- Ref.make(Set.empty[String])                       // receipts currently in hand
    _    <- beat(client, held).repeat(Schedule.fixed(interval)).forkScoped
    loop <- workLoop(client, queue, held)(handle)
  yield loop

/** One beat: renew everything held, and drop whatever the server says we have lost. */
def beat(client: KeyedQueue, held: Ref[Set[String]]): Task[Unit] =
  for
    receipts <- held.get
    reply    <- client.heartbeat(receipts.toSeq)
    _        <- held.update(_ -- reply.stale)                 // stop working those; their result is void
  yield ()

def workLoop(client: KeyedQueue, queue: String, held: Ref[Set[String]])(
  handle: Envelope => Task[Unit]
): Task[Nothing] =
  (for
    reply <- client.dequeue(queue, maxWait = 20.seconds)
    _     <- ZIO.foreachDiscard(reply.delivery): delivery =>
               held.update(_ + delivery.receipt) *>
                 handle(delivery.envelope).exit.flatMap: outcome =>
                   client.settle(delivery.receipt, if outcome.isSuccess then Done else Failed) *>
                     held.update(_ - delivery.receipt)
  yield ()).forever
```

Nothing in that loop needs to know a key exists. Per-key serialisation is the *server's* guarantee, which is
the whole point: a consumer that ignores keys entirely still gets ordered, non-overlapping delivery per key.

The one obligation the heartbeat adds is bookkeeping: hold a set of receipts, add on delivery, remove on
settle, and honour `stale` by abandoning that work. A consumer that beats but never reads `stale` keeps
running work it no longer owns — which the fencing token makes harmless to the *queue*, but not to whatever
side effect the handler was about to perform.

## Failure, and who covers it

| What fails | Covered by | The client sees |
|---|---|---|
| client dies mid-handler | the lease expiring (§`watchdog`, sweep 1) | redelivery elsewhere, `attempt` incremented |
| client is slow, not dead | `Heartbeat` on the tick | nothing, as long as its beats land |
| dkq dies mid-claim | the worker-liveness sweep (sweep 2) | the message is delivered to someone else |
| the client settles late, after a reclaim | the fence token | `APPLIED_STALE` |
| a message that always fails | **nothing yet** — see below | the same message, forever, at the head of its key |

## Deliberately absent in phase 1

- **Streaming and credits** — phase 2. It replaces `Dequeue`'s blocking wait and makes "how much work can I
  take" explicit rather than one-at-a-time.
- **Batch dequeue.** One message per call. Batching interacts with per-key ordering in ways worth designing
  once, not twice.
- **Multi-queue subscribe.** One queue per `Dequeue`, because `BLMOVE` takes one source
  ([`redis-keyed-queue.md` §3b](./redis-keyed-queue.md)).
- **Priorities, delayed enqueue, scheduled messages.** All plausible, none needed to answer phase 1's
  question.

## The gap phase 1 must not ship without

**Poison messages.** `attempt` is in `Delivery` precisely so this can be decided, and per-key ordering makes
it urgent: a message that always fails sits at the head of its key and blocks every later message for that
key, indefinitely. The minimum is a `max_attempts` per queue plus one of: drop, dead-letter to another
queue, or park the key and alarm. It is an API-visible choice — clients need to know what happened to a
message they will never see again — so it belongs in this document before the first deployment, not after.

## Open questions

1. Does `Enqueue` need to be idempotent on `message_id`? (Redis has no unique index; it would need a
   `seen:<key>` set with a TTL, which is a real cost.)
2. Should `Dequeue` accept several queues now, knowing `BLMOVE` cannot? Deciding it here avoids a breaking
   change when phase 2 arrives.
3. `Heartbeat` naming a receipt whose lease has already expired: always report it `stale`, or revive it when
   the key has not been re-claimed since? Reporting it is simpler to reason about and matches `Settle`.
4. Is `key_depth` worth returning at all, or is it a metric that invites clients to make decisions from a
   number that is stale the moment they read it?
