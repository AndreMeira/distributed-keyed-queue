---
title: "The zio-grpc module that was up to date and empty"
type: session
status: current
updated: 2026-08-31
tags: [sbt, zinc, scalapb, zio-grpc, codegen, build, open]
---

# The zio-grpc module that was up to date and empty

Open. Parked deliberately: it surfaced in the middle of the `validation.parse` work, has a working
one-command fix, and chasing it then would have meant stopping the refactor. Picked up once the parse
boundary is finished across all four requests.

## What happens

`modules/protocol-zio-grpc` reaches a state where sbt considers it up to date while it has compiled
nothing. Its `target/scala-3.8.3/classes/homelab/keyedqueue/v1/` holds only the two copied `.proto`
resources — no class files — while `target/scala-3.8.3/src_managed/main/scalapb/homelab/keyedqueue/v1/`
holds all three generated sources (`KeyedQueueGrpc.scala`, `KeyedQueueServiceProto.scala`,
`ZioKeyedQueueService.scala`).

Everything downstream then fails in whatever way that missing layer happens to surface. All three of these
were the same cause:

```
value KeyedQueueGrpc is not a member of homelab.keyedqueue.v1          (compiling ZioKeyedQueueService.scala)
Not found: ZioKeyedQueueService                                        (compiling QueueService.scala)
Exception occurred while executing macro expansion.
java.lang.AssertionError: assertion failed: class QueueService has non-class parent: TypeRef(... KeyedQueue)
                                                                       (ZLayer macro, GrpcApplication.scala)
java.lang.NoClassDefFoundError: homelab/keyedqueue/v1/ZioKeyedQueueService$KeyedQueueClient$
                                                                       (running GrpcSpec)
```

The last one comes with sbt's own suggestion that the `ClassLoaderLayeringStrategy` is at fault. It is not
— there were no classes to load.

## The fix, when it happens

```
sbt protocolZioGrpc/compile
```

Then the normal `sbt test` works. A `clean` alone does **not** fix it and made it worse twice, because
`clean` and `compile` in one sbt invocation is itself suspect — the source list appears to be computed
before protoc regenerates. Run them as separate invocations if a clean is needed at all.

## What was caught in the act, later the same day

Narrowed while converting `Dequeue`, when an `e2e` run failed with the `NoClassDefFoundError` above:

- **The class files are produced.** `sbt protocolZioGrpc/compile` followed immediately by
  `find modules/protocol-zio-grpc/target -name '*.class' | wc -l` gives **31**. Compilation is not
  silently doing nothing.
- **Nothing downstream deletes them.** `server/Docker/publishLocal` leaves 31. `e2e/test` leaves 31, and
  passes. The two halves of `sbt e2e` are fine on their own.
- **The module recompiles on every invocation.** `sbt "show e2e/Test/fullClasspath"` — a query, not a build
  — printed `compiling 3 Scala sources`. So the module never records itself as up to date, and each
  invocation starts by rebuilding it.

That last point is the sharp one, and it reframes the bug: this is not "sometimes the classes are missing"
but "the module's analysis is never valid", with a missing `classes/` as the occasional visible symptom.
It also fits the first suspicion below — a module whose inputs zinc cannot see is a module whose analysis
zinc cannot record.

Practical consequence while it is unfixed: run `sbt e2e` as its two steps (`server/Docker/publishLocal`,
then `e2e/test`) if it fails, before believing the failure is yours.

## Why this module and not the others

Two things about it are unusual, and the fix should start by ruling them in or out:

- **It reads another module's sources.** `Compile / PB.protoSources := Seq((protocol / Compile /
  sourceDirectory).value / "protobuf")` — the `.proto` files live in `modules/protocol`. If zinc does not
  track that directory as an input to *this* module, nothing about editing a proto, or cleaning the other
  module, tells it there is work to do.
- **Both modules generate into `sourceManaged/scalapb`**, split by filter — `protocol` excludes
  `*_service.proto`, `protocol-zio-grpc` includes only it. Between them each proto is generated exactly
  once (see [`../learning-material/proto-generation.md`](../learning-material/proto-generation.md)), but
  the two write to identically-shaped paths under different targets.

Neither is proven; both are where to look.

## Why it matters beyond the annoyance

It is silent. A module that compiles nothing and reports success is indistinguishable from one that has
nothing to do, and the failures it causes all point somewhere else — at the ZLayer macro, at a classloader
strategy, at a member that "is not" on a generated object. Every one of those sends a reader to the wrong
file.

It has not been seen in CI, where every run starts cold. That is not reassurance: a cold build is exactly
the case where the module has to generate *and* compile in one go, which is the path that failed here after
a `clean`.

## Where to pick it up

1. Reproduce deliberately: `sbt clean` in its own invocation, then `sbt test`, and look at whether
   `protocol-zio-grpc/target/**/classes` has class files before blaming the error that appears.
2. Check whether `PB.protoSources` pointing at another module registers as a zinc input — `sbt
   "show protocolZioGrpc/Compile/sources"` and `"show protocolZioGrpc/Compile/unmanagedSources"` after a
   clean, before a compile.
3. If it does not, the options are to make the dependency explicit (a task dependency on `protocol`'s
   proto directory) or to stop crossing modules — generate both from a shared proto source directory that
   each module declares for itself.
