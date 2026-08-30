---
title: "Look-ahead and discard — conflating a key's backlog without a batch API"
type: research
status: draft
updated: 2026-08-30
tags: [look-ahead, conflation, backlog, discard, proto, lua, exploration]
---

# Look-ahead and discard

> **Nothing here is built.** This is a design worked out far enough to cost, with two alternatives rejected
> for reasons worth keeping. The proto shapes below are proposals, and one contract question is still open.

## The problem

Several messages queue behind one another on the same key, and an older one is often superseded — five
"entity 42 changed" events where only the last matters. Today a consumer must claim, work, and settle each
one in turn, doing the work five times.

**The win is skipping work, not saving round trips.** That follows from what the queue costs: handler time
dominates and the substrate has orders of magnitude of headroom
(`throughput-first-numbers.md`). A design that saved round trips and not work would be solving the cheap
half.

## The shape

Two additive fields:

```proto
message Delivery      { … uint32 backlog_depth = 5; }   // still queued for this key, at claim time
message SettleRequest { … uint32 discard_ahead = 4; }   // on DONE, also drop the next N
```

No field on `DequeueRequest`: the depth is always returned, since it costs one `LLEN`.

A consumer conflates like this:

1. Claim #1, see `backlog_depth = 4`.
2. Do **no** work — something newer exists, so this one is stale by construction.
3. Settle `DONE` with `discard_ahead = 3`, dropping #2–#4.
4. The next dequeue returns #5 with depth 0. Do the work once.

Two claim/settle cycles and one unit of work, against five and five.

**Why it is safe — and what the claim does *not* buy.** Holding a claim keeps other consumers off the key,
but `produce.lua` RPUSHes to `msgs` whatever the key's state, so **a producer may append while the claim is
held**. `backlog_depth` is therefore a lower bound, not a snapshot: it can grow between the claim and the
settle, and never shrinks, since nothing but the holder may take from the list.

That is exactly why `discard_ahead` counts **from the head**. Appends land at the tail, so dropping N from
the head drops the N that were reported and can never reach a message that arrived afterwards. Had it
counted from the tail, or been expressed as "empty the key", a concurrent append would be silently lost.

## Why depth is enough, and headers are not free

The first shape had `repeated MessageHeader ahead` — ids, types and timestamps of the next N, "no payloads".
**That is not implementable cheaply**, because `msgs:<key>` stores whole serialised messages: `produce.lua`
pushes `StoredMessage.toBytes(message)`, payload included. An `LRANGE` therefore returns full messages, and
neither Redis nor Lua can strip them. The options are:

- **parse and strip in the adapter** — N protobuf parses per dequeue, on the hot path, to discard most of
  what was parsed; or
- **a parallel `msgs:<key>:ids` list** — cheap to read, but `produce.lua`, the failed-settle path in
  `complete.lua` and the watchdog's reclaim must then move two lists in lockstep, and any path that drifts
  leaves a silently wrong look-ahead.

Neither earns its place, because **depth alone answers the question latest-wins asks.** If depth > 0 this
message is superseded; nothing about *what* is behind it changes the decision. Headers would only buy
*selective* decisions — "the message behind is a delete, so skip mine" — and no such case exists yet.

Deferring them is cheap: both fields are additive on the wire, and the storage-format question can be faced
if a selective case appears.

## Why not batch-claim the whole backlog

The natural-looking alternative: claim a key's whole backlog under one token, hand the consumer a list, let
it settle once. It is the better design for saving round trips — and round trips are the half that does not
matter here. What it costs:

- **partial failure** — a consumer that processed three of five needs `done_through`, and the contract grows
  a second way to settle;
- **attempt counting across a batch** — currently per head message, and a poison message is visible because
  its count climbs;
- **returning a partly-processed batch** to the head of `msgs` in order, on a `FAILED` settle.

Look-ahead plus `discard_ahead` gets the conflation win while touching none of it.

## Cost

A focused day, mostly mechanical:

| where | change |
|---|---|
| `keyed_queue.proto` | two additive fields |
| `consume.lua` | one `LLEN msgs`, returned as a 5th element |
| `complete.lua` | one `ARGV[5]`, one `LTRIM msgs n -1` in the `done` branch |
| `ConsumeScript` | decoder `sized(4)` → `sized(5)`, one more `Decode.long.at(4)` |
| `CompleteScript` | one more `args` entry |
| `Claimed`, `Delivery` | one field each |
| `QueueStore.settle` | `+= discardAhead` — `claim` is untouched |
| `Scripts`, `RedisQueueStore`, `SettleUseCase`, `DequeueUseCase` | pass through |
| `QueueInputValidation` | cap `discard_ahead` |
| tests | ~2 in `QueueStoreSpec`, 1 in `GrpcSpec` |

Leases, fencing, the claiming path and the watchdog are all untouched — which is what keeps it a day.

## The one hazard

**`LTRIM` placement in `complete.lua`.** It belongs inside the `done` branch, after `DEL inflight`, and
**before** the existing `LLEN msgs == 0` check that decides whether the key goes back on `ready` or becomes
idle.

Put it after, and discarding a key's whole remaining backlog leaves that key sitting in `ready` with no
messages behind it. Nothing repairs that: the watchdog's sweeps look for lapsed claims, dead workers and
elapsed backoffs, and this is none of them. A consumer would claim the key, find nothing, and the claim
would resolve to `None` — wasted round trips for ever, with no error anywhere.

## Open

- **Does `discard_ahead` count, or name a `message_id` to discard through?** Counting is simpler and matches
  what the depth reported. An id is safe against a caller miscounting, but makes the server verify it
  against the head of `msgs` and decide what to do when it does not match. Worth settling before the proto
  ships, now that `protocol-wire` is publishable and field semantics become a contract.
- ~~**Cap `discard_ahead`.**~~ Settled: it is validated as non-negative, not capped. Asking for more than
  remain is legitimate — the backlog may have drained since the depth was read — and `LTRIM` clamps. A
  *negative* is neither: `uint32` decodes to a signed `Int`, so a wire value at or above 2^31 arrives
  negative, and `LTRIM` reads a negative start as an offset from the end. A discard of -2 would keep the
  last two messages and drop everything before them. Refused in `QueueInputValidation`.
- **Say in the contract that the queue never conflates on its own.** The consumer decides a message is
  superseded; the broker only reports depth and drops what it is told to. "Look-ahead" invites the opposite
  assumption, and someone will eventually expect log-compaction semantics.
