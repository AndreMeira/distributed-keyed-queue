---
title: "Checkpoint: the redis package back in shape, and where readiness belongs next"
type: session
status: current
updated: 2026-09-13
tags: [checkpoint, redis, partitions, readiness, connections, key-layout, cleanup, ports]
---

# Checkpoint: the redis package back in shape

A long refactoring session on `infrastructure/redis/`, ending with the package in an acceptable shape and
one clear next move. Written to say what changed, what it cost, and what to pick up first.

## What landed

Six commits, `a7d4ec5`..`d6a30cc`, unpushed at the time of writing.

- **Queues and locks share a partition.** The lock's own `{l:N}` tag space is gone: a lock's keys live
  under `{p:N}:v3:l`, and both APIs announce on the partition's single wake stream. Entries carry a `kind`
  field (`q` / `l`), so one stream feeds two sinks without either seeing the other's wakes. This halved the
  streams, the slots and the blocking connections a cluster holds.
- **`Waker` is gone.** `ReadinessListener.Kind` carries the name at the type its kind implies
  (`Queue(QueueName)` / `Lock(LockName)`), so delivery is an exhaustive match rather than a lookup table,
  and a sink can no longer be handed the other's name. `Readiness`/`Broadcast` became
  `QueueReadiness`/`LockReadiness`.
- **`KeyLayout` is a class.** It holds the partition count, mints tags, and answers which partition a name
  falls in. The `GroupId` / `groupOf` / slot-computing machinery is deleted: a partition owns exactly one
  wake stream and exactly one blocking connection, on every deployment.
- **The count is the deployment's.** A cluster uses sixteen partitions; a single server uses one, because
  it has no slots to spread across. Measured on a running instance: one blocked `XREAD` and one shared
  connection, against sixteen blocked readers before the change.
- **The marker records the layout, not just the version** (`v3.p16`), since the count now varies. An
  instance that disagrees refuses to boot — the guard that had been parked as an idea.
- **`layout accept` is gone.** An ops mode needing its own deployment to run once was worse ergonomics than
  the remedy it replaced: drain, `DEL dkq:layout:schema`, start. `KeyLayout.accept` went with it.
- **Lock documentation reached parity with the queue's**: `architecture/lock-mechanics.md` (states, the
  script per transition, how a waiter waits, the code map) and `learning-material/lock-state-walkthrough.md`
  (every call, key by key).

## Where to resume

**The readinesses belong in the domain, and the listener should be a port.** `QueueReadiness` imports
exactly `domain.types.QueueName` and `zio.*`; `LockReadiness` the same with `LockName`. Neither touches
Lettuce, `Connection` or `RedisKey` — they are pure in-process coordination, a token buffer and a mailbox
map. `ReadinessListener` has ~16 references to Lettuce and the connection: it is the adapter.

So: the two readinesses move to domain level (they use ZIO effects, so `domain/service/`, not
`domain/model/`), and `ReadinessListener` becomes a port with `RedisReadinessListener` implementing it.
`docs/research/postgres-substrate.md` already named this seam — "`QueueReadiness` is already
substrate-agnostic" — and sketched a `PgReadinessListener` driving the same readiness. Making the seam real
is what turns a second substrate into one new adapter.

### The move is bigger than it looks, and that is the point

Moving the two files compiles, and would still be wrong. **Every reference to the readinesses today is in
`infrastructure/redis/`** — both stores, the listener, the module. Move them alone and nothing in `domain/`
mentions them: they would be citizens nobody there talks to, and a reader of the domain package would
rightly ask what they are doing in it. No formal dependency on infrastructure, but a conceptual one.

**The readinesses belong in the domain only if the waiting moves with them** — and the waiting is already
domain logic sitting in an adapter. Eleven of `RedisLockStore`'s sixteen methods never touch a script:
`queued`, `awaitTurn`, `nextEventIn`, `untilRecheck`, `atLeastFloor`, `remainingTime`, `hold`. Hold a
ticket, park until the next known event or until the patience runs out, withdraw on any exit that is not a
grant — none of it is Redis, all of it is the lock's contract. `RedisQueueStore`'s `claimWithin` →
`attemptClaim` → `awaitReady` loop is the same shape.

So the cut is:

- **domain owns waiting** — attempt, park on a readiness, retry until patience. That is what uses the
  readinesses, and it is the thing whose contract mentions patience in the first place.
- **the ports shrink** to what only a substrate can answer: attempt one claim or one grant, and tell me
  when a name changed — the listener port.
- **the adapters** become script calls and codecs.

### Start here: express the listener as a `Processor`

The first step is a shape, not a dependency. `ReadinessListener` becomes a `Processor` from
`homelab.common.processing` — `input: Consumer[E, A]` plus `process(value)` — with the Redis specifics
isolated behind the `Consumer` contract:

```
Consumer[RedisFailure, StreamMessage]   <- toolkit contract
  \- dkq's own tail implementation       <- the XREAD, the offsets, the per-partition connections
Processor                                <- decode kind + name, match, deliver
  \- QueueReadiness / LockReadiness       <- unchanged
```

**Nothing new is needed to do it.** `Consumer` and `Processor` are in `homelab-common`, which dkq already
depends on at 0.0.3 — the classes are in the cached jar. The consumer implementation stays dkq's own for
now; whether it is ever replaced by the toolkit's `StreamTailConsumer`/`ClusterStreamTail` is a later swap
behind the same contract, and not a prerequisite. Those two live in `incubator`, which is
`publish / skip := true`, so adopting them would mean promoting them first — a separate decision, and one
this codebase's version would inform rather than wait on.

What that buys immediately: everything substrate-specific ends up behind one `consume` method, and the
processor half is the ~20 lines that decode an entry and call a readiness — with no Lettuce in it, and so
nothing left holding it in `infrastructure/`.

**The one thing to design rather than port.** This listener catches a read failure, calls `readyAll` on both
sinks — a level-triggered backstop, since `XREAD` does not report entries trimmed while it was away — then
backs off and never dies. `Processor.run` aborts on the first failure and expects a graph to restart it. So
the announce-everything step has to be expressed inside `process`, or in a wrapper around the loop; it is
dkq semantics that no generic consumer supplies, and it is the property most easily lost in the move.

The consumer can own the failure — retry and back off behind `consume`, so nothing reaches the processor —
and that is what makes the change incremental. But it must not swallow it silently: the consumer is the only
thing that knows it '''lost its place''', so it should say so as a value, delivering a *gap* element
alongside real entries. The processor then matches — an entry goes to its readiness, a gap means `readyAll`
on both — and the readinesses stay on the processor side, unknown to the consumer. That is also tidier than
today, where one `catchAll` conflates "the read failed" with "re-announce everything".

These two moves are halves of one boundary, not two independent jobs: the listener port is the inbound half,
the waiting loop is what consumes it. Doing either alone leaves an unused domain type or a substrate-free
listener with nothing on the domain side to hand to.

## Numbers, and how they were taken

The `ThroughputSpec` sweep after the refactor, against an image built from `HEAD`, with the September
baseline from [`../research/throughput-first-numbers.md`](../research/throughput-first-numbers.md) beside it:

| keys | consumers | msg/s (2026-09-13) | msg/s (2026-09-05) |
|-----:|----------:|-------------------:|-------------------:|
| 8    | 8         | 1,212              | 1,552 |
| 16   | 16        | 1,633              | 1,896 |
| 64   | 8         | 1,724              | 2,048 |
| 64   | 16        | 2,462              | 3,160 |
| 64   | 32        | 3,453              | 4,339 |

**The refactor did not cost this.** Two further sweeps on an older image landed in the same band (8×8:
1,071 and 1,062 — *below* the refactored build), so three sweeps across two code versions agree. What
differs from September is the machine: load average 4–8 all evening, and `dkq-jaeger` holding 3.6 GB of the
box's 7.65 GB while the sweep ran. Settling the comparison needs a quiet box, telemetry down.

The curve — the only thing this indicator is for — is unchanged. Consumers bind, not keys: 8→32 consumers
at 64 keys buys **+113%**, against +112% in September.

## Also open

- **`listener.run` is forked with a bare `forkScoped`** in `Module`, and it can now fail (a partition with
  no connection). A dead listener is silent, which is what its own doc says must never happen; `.orDie`
  would at least make it a reported defect.
- **`sbt e2e/test` does not build the image; `sbt e2e` does.** `end-to-end-testing.md` says so, and this
  session ignored it for hours — every "e2e green" before the final run was taken against a five-hour-old
  image and verified nothing about the day's changes. The closing runs (15/15 standalone, 15/15 cluster)
  used the alias and are the ones that count.
- **A reproducible build trap.** `sbt "protocol/clean" "protocolZioGrpc/clean" "protocol/compile"
  "protocolZioGrpc/compile"` in one invocation fails with 263 errors — generated sources referring to
  generated types. The same commands in separate invocations succeed. This cost several confusing test runs
  and is the likely cause of the day's `NoClassDefFoundError` "flakes" in `GrpcSpec` and the e2e `LockSpec`.
- **An unexplained intermittent.** `RedisLockStoreSpec` failed two tests on two occasions, both on the first
  full-suite run after an edit, never reproduced, and the failing test names were never captured. Capture
  the raw log first if it recurs.
- **A trade to keep in view.** On a single server every wake — queue and lock, every name — now funnels
  through one stream capped at `MAXLEN ~1000`, where before there were sixteen such buffers. Losing wakes
  costs latency (waiters fall back to their recheck deadline), never correctness.
