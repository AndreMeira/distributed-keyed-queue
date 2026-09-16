---
title: "Checkpoint: spec organisation, the lock sketch removed, and the scaladoc sweep to come"
type: session
status: current
updated: 2026-09-15
tags: [checkpoint, testing, spec-support, distributed-lock, scaladoc, documentation, cleanup]
---

# Checkpoint: specs reorganised, a dead sketch removed, and a documented plan for the scaladoc

Branch `spec-refacto`, off `main` at `1fec281` (the `module-init` merge, PR #14). Two things landed; the
third — the largest — is a plan, written down here because the sweep it describes should not start from
memory.

## What landed

### Test files mirror main (`cde277c`)

Four specs sat at the root of the test tree and moved into the package of what they test: `GrpcSpec` to
`application/grpc/v1/`, `QueueConfigSpec` to `infrastructure/configuration/`, `KeyLayoutSpec` to
`infrastructure/redis/keys/`, and `QueueStoreSpec` to `infrastructure/redis/RedisQueueStoreSpec` — renamed
in both file *and* object, since the earlier `QueueReadinessSpec.scala` holding `object ReadinessSpec` came
from renaming only one of the two.

**`RedisSpecSupport` is the new per-package support object**, following
`zio-conduit-doobie`'s `PostgresSpecLayers` with its layers and aspects collapsed into one object, as
agreed for dkq and future homelab services. It carries what the five substrate-backed specs had been
copying verbatim:

| member | replaces |
|---|---|
| `substrate(leaseTtl, port)` | five copies of the Valkey container block, `api.version` pin included |
| `instance(config)` | three hand-rolled replicas of the adapter's wiring |
| `connection(config)` | the bare connection `KeyLayoutSpec` needs — it must *not* get a started instance |
| `againstValkey` | `withLiveClock @@ sequential @@ timeout(3.minutes)`, five times |
| `layout`, private `config(...)` | `KeyLayout.of(cluster = false)`, and a 12-positional-argument `QueueConfig(...)` no reader could decode |

The load-bearing part is `instance`, which builds a pod the way the composition root does rather than
imitating it:

```scala
provided <- Module.layer.build.provideSome[Scope](ZLayer.succeed(configured), monitor)
_        <- Module.init.provideSome[Scope](ZLayer.succeedEnvironment(provided), ZLayer.succeed(configured))
```

Two calls give two independent instances, which is what the contention and cross-instance tests need, and a
step added to the adapter's startup now reaches every spec. This is the accepted `init` trade-off
(load-bearing, silently omissible) paying off: it had broken the specs twice in the preceding week, each
time as a hand-copied startup sequence that had gone stale.

Net `-236/+59` across the test sources. 73 tests green at that commit.

**Only one package earned a support object.** `domain/service/validation/` shares nothing between its two
specs but a `30.seconds` literal; the other packages hold a single spec each. `e2e/` already had this shape
— `Deployment`, `Compose`, `Instance`, `Consumer` — so the server tests were the outlier, not the pioneer.

One deliberate exception to the mirroring: **`GrpcSpec` has no counterpart type in main.** Its subject is a
running process — it calls `serve`, binds a port, dials it. Either it becomes `GrpcApplicationSpec` or it
stays the one spec named for a surface rather than a class. Undecided.

### The self-client lock sketch is gone (uncommitted at the time of writing)

`DistributedLock` and `DistributedLockSpec` are deleted. It came up because its companion carried a
`LockName` of its own, shadowing `domain.types.LockName` for the whole file (the file imports
`domain.types.*` at the top and `DistributedLock.*` inside the class). Both are `opaque type Type <: String
= String`, so nothing ever caught the confusion.

The reason the duplicate survived is the reason the file went: **nothing in `main` referenced it.** Written
2026-09-10 as a sketch (`5435029`), superseded the next day by the dedicated store (`ca6838c`), and its only
caller ever was its own spec. The gRPC lock is `LockStore` → `RedisLockStore` → the three lock use cases.
`Watchdog.watch` keeps its real callers (`EnqueueUseCase`, `DequeueUseCase`), so nothing was stranded.
`docs/research/distributed-lock.md` gained a dated addendum: its preamble had claimed both sketches were in
the tree, and its recommendation ("start with the self-client, graduate later") is now recorded as not what
happened. 68 tests green after the removal.

## The scaladoc problem, and the sweep to come

Raised on `RedisModule`'s `'''The connection split is the thing to notice.'''` — the docs across the
codebase are, in too many places, a rephrasing of the reasoning that produced the code or of the request
that prompted it, rather than something a caller can use.

### What is actually there

`modules/server/src/main` holds **487 scaladoc blocks**. Most are fine: 360 are 1–3 prose lines, the
`@param`/`@return` method docs. It is concentrated in **42 blocks of 6+ lines across 14 files**, and it has
one recognisable shape:

| marker | hits | signals |
|---|---|---|
| `'''…'''` bold lede | 69 spans, 31 files | a thesis, argued |
| "rather than" | 83 | comparison with a design that does not exist |
| "would " | 50 | a counterfactual |
| "deliberate" / "on purpose" / "which is why" | 24 | justification |

**The bold is the diagnosis.** Bold marks a claim being argued; a doc has no claims to argue, only facts to
state. A doc that opens `'''One token, one consumer.''' This is the point of the design` has stopped
addressing a caller and started addressing a reviewer deciding whether to approve the design. That reviewer
existed once; the text outlived them. `QueueReadiness` is the extreme at 21 prose lines, five bolded theses,
ending in a comparison with a promise-based implementation that no longer exists.

### Why the rules already in CLAUDE.md do not bite

They are judgement rules — "could a caller tell?", "document the thing, not its circumstances" — applied at
write time by the author who has just finished the reasoning, when it is warm and reads as insight. They
are then never checked again. What is missing is a mechanical rule and a post-write check.

### Proposed

1. **No `'''` in a scaladoc — none.** Greppable, needs no taste, and removes the container the essay travels
   in. Each of the 69 spans is then either a fact that survives as plain prose or rationale that leaves.
2. **The opener rule: the first sentence names what the thing is or does**, never a property of the design.
   "The queue over Redis." is right; "The connection split is the thing to notice." is a reviewer's sentence.
3. **A gate, not a principle.** Before finishing, grep the diff for
   `'''|rather than|instead of|would |otherwise|on purpose|which is why|deliberate|worth`, and answer every
   hit: keep (caller-observable), move, or cut. `bin/doc-lint` could fail on the bold and warn on the rest —
   it turns "audit 487 blocks" into "triage ~100 hits".

### Relocate, do not delete — and some of it is already relocated

The example that makes the case:

```scala
/**
 * Wiring for the Redis adapter.
 *
 * '''The connection split is the thing to notice.''' One shared connection serves everything that must
 * never park, and a pool of connections serves the one operation that must — so a idle claim cannot stall
 * an enqueue. …
 */
```

`docs/architecture/redis-connections.md` already documents exactly this, at length, with measurements. So
the scaladoc is a second, undated, unrevisable copy of a doc that exists — and when the connection design
changes, only one of them gets updated. The target shape is "what it supplies, plus a pointer":

```scala
/**
 * Wiring for the Redis adapter.
 *
 * Supplies both stores over the connections in `Connection` — see `docs/architecture/redis-connections.md`.
 */
```

The redis package will want a `queue-mechanics.md`, peer to the existing `lock-mechanics.md`, to absorb what
comes out of `QueueReadiness` and `RedisQueueStore`.

### The sweep, ranked by marker density

```
21  application/grpc/v1/QueueService.scala      12  infrastructure/redis/keys/QueueKeys.scala
20  infrastructure/redis/RedisQueueStore.scala   9  domain/service/lock/LockStore.scala
18  infrastructure/redis/script/LuaScript.scala  8  infrastructure/redis/LockReadiness.scala
18  infrastructure/redis/QueueReadiness.scala    7  infrastructure/redis/script/QueueScripts.scala
15  infrastructure/redis/RedisLockStore.scala    7  infrastructure/redis/keys/LockKeys.scala
14  infrastructure/redis/keys/KeyLayout.scala    7  infrastructure/redis/RedisFailure.scala
13  infrastructure/redis/WakeConsumer.scala      7  domain/service/usecase/v1/HeartbeatUseCase.scala
```

Four or five batches, one per package, each its own commit so the deltas stay readable, each with an
explicit list of what moved to `docs/architecture/` rather than being dropped.

### Open before it starts

- Is **"no bold in scaladoc, full stop"** the rule to write into `CLAUDE.md`? It is the one that makes this
  self-enforcing.
- Does displaced reasoning land in `docs/architecture/` as the sweep goes, or in a scratch file to be placed
  by hand afterwards?

## Where to resume

1. **The scaladoc sweep**, above — answer the two questions, then work the ranked list.
2. **`docs/architecture/lock-mechanics.md` still names `ReadinessListener`**, twice, including in its
   component table. That type is now `WakeConsumer` + `ReadinessProcessor`. It is the live architecture doc
   for the lock that ships, so it is the most wrong doc in the tree; it needs a call on how to present the
   consumer/processor split.
3. **Uncommitted**: the `DistributedLock` removal and its research-doc addendum.
4. Still parked from 2026-09-13: **the readinesses move to the domain** and the listener becomes a port —
   see that session's log, and `docs/research/waiting-in-the-domain.md`.


## Addendum (2026-09-16): the spec harness, and the bug that cost the evening

### What the harness looks like now

One support object per package, plus one shared vocabulary:

- **`RedisSpecSupport`** — `container` (`ZLayer.scoped(Container.run)`), `config(lease)`, `layer`
  (`monitor >+> RedisModule.layer`, taking `QueueConfig` as input), and `Aspect.init`.
- **`GrpcSpecSupport`** — its own `port` and `config` (borrowing `RedisSpecSupport.container`), the two
  clients as `layer`, and an `Aspect.init` that serves.
- **`SpecHelper`** — `Aspect.common`, `Failure`, and `Helper`, which now holds every helper the specs used
  to keep private. Three specs each had a `message`; the two non-domain ones became `wireMessage` and
  `requestMessage`, because the wire and request versions have identical parameter lists and cannot overload.

A suite reads: `) @@ <Support>.Aspect.init @@ SpecHelper.Aspect.common` then
`.provideSomeShared[Scope](<Support>.config(...) >+> <Support>.layer)`. `RedisSpecSupportOld` is deleted;
68 tests green.

### The bug: a fiber forked in an acquire cannot be interrupted

Symptoms were a test JVM that never exited, a Valkey container that was never stopped, and a `stopped()`
that never ran. The chain:

1. `TestAspect.beforeAll(effect)` is `aroundAll(effect, ZIO.unit)`, and `aroundAllWith` is
   `Spec.scoped(ZIO.acquireRelease(before)(after))`. So the effect runs as an **acquire**.
2. `ZIO.acquireRelease` runs acquire **uninterruptibly**, and ZIO's rule is that *a forked fiber inherits the
   interruptibility of the region it was forked in*. `RedisModule.init`'s `forkScoped` readers were therefore
   born uninterruptible.
3. zio-test closes the suite scope after the last test — it does, and it warns when it cannot finish:
   `"ZIO Test is attempting to close the scope of suite … more than 60 seconds … may indicate a resource leak"`.
   **That warning is the signal for this whole class of bug**, and it names the suite.
4. The close waited on fibers that could never receive the interrupt; nothing behind it in the finalizer
   chain ran, so Lettuce stayed up, ryuk kept the container, and the JVM never exited.

The fix is `RedisModule.init.interruptible` in the aspect. `ZLayer.scoped(RedisModule.init)` also works,
because a layer's effect is not an acquire — and it has the further advantage that teardown order is derived
from the dependency graph.

**Production is not affected**: `serve` calls `RedisModule.init` directly in the app scope, with no acquire
around it, so those four `forkScoped` fibers are interruptible, and they are torn down before the
connections they use because `init` runs after the layers are built. That ordering is a consequence of the
composition-root shape, not something the types enforce. Two follow-ups worth considering:
`.interruptible` on the four forked loops so the property holds regardless of caller, and overriding
`ZIOApp.gracefulShutdownTimeout` (it defaults to `Duration.Infinity`, so a stuck finalizer means the pod
waits for SIGKILL).

### Aspect order decides which clock the inits see

`spec @@ a @@ b` composes as `b(a(spec))`, so the *first* aspect is innermost. With
`@@ SpecHelper.Aspect.common @@ …init`, the `withLiveClock` in `common` wrapped only the tests, and the
`beforeAll` effects ran on the **test clock** — where `ZIO.sleep` waits for a `TestClock.adjust` that never
comes. `common` now goes last in every spec.

### Smaller things learned

- A Valkey-backed spec costs ~50-60s of Testcontainers startup before any test runs; the tests themselves
  are a few seconds. One container per run instead of one per spec is the only lever that matters.
- `GrpcSpec` binds a fixed 19099, so two overlapping runs collide and exactly the gRPC tests fail fast.
- A background `python3 -` edit script from 13 September had been spinning a core for 55 hours on a
  runaway regex. Foreground edit scripts, and literal matching rather than regex, avoid the whole class.

