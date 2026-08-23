---
title: "A keyed queue on Redis — blocking pops, per-key exclusivity, and what it costs"
type: research
status: draft
updated: 2026-08-23
tags: [redis, keyed-queue, blocking, lease, heartbeat, lua, postgres, exploration]
---

# A keyed queue on Redis

> **Exploration, not a decision.** The commands and pseudo-code below are worked out far enough to expose the
> hazards; none of it is a committed API, and Postgres remains the alternative (§6). What this note is for is
> the *shape* of the thing and the price of each option — naming, module layout and the gRPC surface are all
> still open.

Problem statement and the requirement this serves:
[`research/infrastructure/homelab-message-broker.md`](../../../research/infrastructure/homelab-message-broker.md)
— partition ownership, per-partition order, **per-key serial processing with long-lived handlers**, and
concurrency across keys.

## 1. Why this came up: the blocking dequeue

A client's `Dequeue` is a blocking call with a deadline. If it parks on pod A and a message is enqueued on
pod B, pod A has to learn within milliseconds. On Postgres that means `LISTEN`/`NOTIFY` as a hint plus a poll
floor for correctness, because a notification reaches only sessions listening *at commit time* — no backlog,
no replay — so every reconnect gap is a hole and the hint can never be load-bearing.

Redis makes the wake-up a primitive rather than a mechanism: a blocking pop *is* the cross-node signal, and
the server holding the data is the one that wakes you.

## 2. The shape: the queue holds *keys*, not messages

The move that makes per-key exclusivity structural rather than a consumer-side lock: **`QUEUE` is a list of
keys that are ready and not currently being worked**. A key appears in it at most once, so at most one worker
can hold it — the guarantee falls out of the data structure instead of being enforced by a lock somebody has
to remember to take.

Each key then owns its own FIFO of messages, and a small state machine keeps the two consistent:

```
state:<key> in { idle, queued, processing }

QUEUE            list    keys ready to be worked          (BLMOVE blocks here)
CLAIMED          list    keys handed to a worker, mid-transition
claimed          zset    key -> deadline, unix millis     (the lease; the watchdog reads it)
msgs:<key>       list    that key's messages, in order
inflight:<key>   list    the one message being worked
fence:<key>      string  monotonic claim counter
```

`state` makes a separate `WAITING` list unnecessary: "waiting" is exactly `state == processing` with
`msgs:<key>` non-empty, which cannot drift out of sync or accumulate duplicates the way a second list can.

**One direction convention, and it is load-bearing: push at the tail, pop at the head.** `RPUSH` to add,
`LEFT` to take — `RPUSH` plus a `RIGHT` pop is a *stack*, and would silently reverse the per-key ordering
this design exists to guarantee. The single exception is the watchdog restoring an in-flight message, which
pushes to the head on purpose. Verified against Valkey 8.1: with `a b c` pushed via `RPUSH`, a `LEFT` pop
returns `a` and a `RIGHT` pop returns `c`.

## 3. The four operations

Each is one Lua script, because each spans several structures and the interleavings are precisely the bugs.

```
produce(key, msg):
  RPUSH msgs:<key> msg
  IF state:<key> == idle THEN state:<key> = queued; RPUSH QUEUE key

consume():                                -- the block happens outside the script; scripts cannot block
  key = BLMOVE QUEUE CLAIMED LEFT RIGHT <timeout>
  -- then, atomically:
  state:<key> = processing
  INCR fence:<key>                        -- the token this claim carries
  ZADD claimed <now + ttl> key            -- a deadline exists from the instant of the claim
  LMOVE msgs:<key> inflight:<key> LEFT RIGHT
  LREM CLAIMED 1 key

complete(key, token):
  IF fence:<key> != token THEN reject     -- a reclaimed zombie must not land its result
  DEL inflight:<key>; ZREM claimed key
  IF LLEN msgs:<key> > 0 THEN state:<key> = queued; RPUSH QUEUE key
  ELSE                        state:<key> = idle

heartbeat(keys):
  ZADD claimed <now + ttl> key1 key2 ...  -- one call renews every key this worker holds

watchdog():                               -- idempotent, so every pod runs it; no leader election
  FOR key IN ZRANGEBYSCORE claimed -inf <now>:
    LMOVE inflight:<key> msgs:<key> LEFT LEFT   -- back to the HEAD: a retry must not reorder the key
    state:<key> = queued; RPUSH QUEUE key; ZREM claimed key
```

## 4. Why each choice, rather than the obvious one

- **`BLMOVE`, not `BLPOP`.** With `BLPOP` the key exists in no structure between the pop returning and the
  follow-up script running; a worker dying there strands it. `BLMOVE` is blocking *and* an atomic handoff
  into `CLAIMED`, so no such window exists.
- **`LMOVE msgs -> inflight`, not `LPOP`.** The same argument one level down: a popped message is
  unrecoverable if the worker dies, which quietly turns the queue at-most-once.
- **A ZSET of deadlines, not `EXPIRE` per claim.** Redis expiry is invisible — you cannot enumerate what
  vanished without keyspace notifications, which are fire-and-forget, putting us straight back in "was a
  signal missed?" territory. A ZSET makes expiry *queryable*, and re-sweeping is harmless.
- **Per-key deadlines, not a heartbeat inventory to diff.** "Reclaim everything no worker claims to hold"
  requires having heard from *every* live worker first; one late heartbeat and that worker's keys look
  abandoned. Judging each key against its own deadline needs no global knowledge.
- **The heartbeat renews rather than reports.** `ZADD` with many members is one idempotent call, and because
  it is a renewal a long handler simply keeps its key — no lease duration to guess against the longest job,
  which is the usual lease-tuning trap.
- **Lua for every transition.** Otherwise two producers both read `state == idle` and push the key twice — two
  workers on one key. The nastier one: a producer observing `processing` while the worker completes and finds
  nothing waiting, so the key never re-enters `QUEUE` and its messages are stranded. Script atomicity removes
  the class.

## 5. Hazards that survive the design

1. **A missed heartbeat means "I cannot hear you", not "you are dead".** A GC pause or a partition lets the
   watchdog reclaim a key whose handler is still running, and then two workers *are* running it. No timeout
   scheme avoids this — leases are not locks. `fence:<key>` is what keeps it survivable: the zombie's
   completion, and ideally its side effect, is rejected as stale. The honest guarantee is **at most one
   *effective* worker per key**, not at most one running — hence handlers stay idempotent.
2. **A poison message blocks its key forever.** Worse here than in an unordered queue: the failing message
   sits at the head of `msgs:<key>`, so every later message for that key waits behind it through every retry.
   Needs an attempt counter and a policy — dead-letter the message and let the key proceed, or park the key
   and alarm. Decide before the first deployment, not after it.
3. **Cluster mode needs hash tags.** A Lua script may only touch keys in one slot, so names have to be written
   `{<queue>}:msgs:<key>` to co-locate them. Cheap now, invasive later.
4. **A cancelled `Dequeue` is a cancelled claim.** A client deadline or disconnect interrupts the server fiber
   mid-claim — see
   [`homelab-toolkit-zio/docs/sessions/2026-08-22-pollconsumer-orphans.md`](../../../homelab-toolkit-zio/docs/sessions/2026-08-22-pollconsumer-orphans.md)
   for how expensive that is when the claim does not happen in the fiber that will do the work. Here it is
   cheap for the same reason it is cheap in a single-element design: `BLMOVE` returns to the fiber that will
   run the work, and if that fiber dies the deadline reclaims the key.

## 6. What Redis costs, against Postgres

| | Redis | Postgres |
|---|---|---|
| blocking dequeue | native (`BLMOVE`) | `LISTEN`/`NOTIFY` hint + poll floor |
| per-key exclusivity | structural — a key is in `QUEUE` at most once | a `keys` table claimed with `SKIP LOCKED` |
| lease / recovery | hand-rolled ZSET + watchdog | `claimed_until` column + reaper |
| multi-structure atomicity | Lua scripts | one transaction |
| durability | async replication, AOF `everysec` — a failover can lose ~1s of acknowledged writes | synchronous replicas + PITR, already running under CNPG |
| operational cost | **a new stateful service** — not in the cluster today | already deployed |

The durability row is the one that decides it. The rest is taste and line count; "we acknowledged your job
and then lost it" is a different kind of statement to make about a job queue.

## 7. What the POC should settle

1. Does the keyed shape hold up end to end — enqueue, blocking dequeue, per-key serial, concurrency across
   keys — over gRPC and across pods?
2. What does reclaim cost in practice: how often does the watchdog fire under normal churn, and does the
   fencing token ever reject anything?
3. Is head-of-line blocking per key acceptable for the workloads in mind, or does a key need a
   parallelism > 1 mode?
4. Only then: is the durability trade acceptable, or does this shape want re-implementing on Postgres with
   the same state machine and `SKIP LOCKED` doing what `BLMOVE` did?

A port boundary is what keeps (4) open: the state machine above is substrate-agnostic, and the only thing
Redis really contributes is that the waiting is free.
