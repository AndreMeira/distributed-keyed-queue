---
title: "The zio-grpc module whose class files disappear mid-test"
type: session
status: current
updated: 2026-08-31
tags: [sbt, zinc, scalapb, zio-grpc, codegen, build, race, open]
---

# The zio-grpc module whose class files disappear mid-test

Open. Investigated far enough to characterise, not far enough to name the actor. Parked with the evidence
written down because it stopped reproducing, and a fix that cannot be validated is not a fix.

## What happens

`modules/protocol-zio-grpc` ends an sbt invocation with `target/scala-3.8.3/classes/homelab/keyedqueue/v1/`
holding only the two copied `.proto` resources and **no class files**, while `src_managed/` holds all three
generated sources. Everything downstream then fails in whatever way that missing layer happens to surface:

```
value KeyedQueueGrpc is not a member of homelab.keyedqueue.v1     compiling the generated stub
Not found: ZioKeyedQueueService                                   compiling QueueService.scala
AssertionError: class QueueService has non-class parent           the ZLayer macro
NoClassDefFoundError: …ZioKeyedQueueService$KeyedQueueClient$     running GrpcSpec / e2e
```

sbt's own advice on the last one points at `ClassLoaderLayeringStrategy`. It is a red herring — there were
no classes to load.

**The fix, when it happens:** `sbt protocolZioGrpc/compile`, then the command again. If `sbt e2e` fails, run
its two halves (`server/Docker/publishLocal`, then `e2e/test`) before believing the failure is yours.

## It is a race, and this is the evidence

- **The class files are produced correctly, in the right order.** From the failing run's own log:

  ```
  Compiling 1 protobuf files to protocol/…/src_managed
  Compiling 1 protobuf files to protocol-zio-grpc/…/src_managed
  compiling 15 Scala sources to protocol/…/classes
  compiling  3 Scala sources to protocol-zio-grpc/…/classes    ← produced
  compiling 45 Scala sources to server/…/classes
  compiling  4 Scala sources to server/…/test-classes
  [tests start] → class files vanish here
  ```

- **They are deleted during test *execution*.** A 0.3s poller recorded `31` at 19:43:06 and `0` at
  19:43:20, after the last "done compiling" and while specs were running.
- **The deletion is best-effort and silent.** `chflags uchg` on the class files mid-run: they survived, the
  build passed, and **no error appeared anywhere in the log**. Whatever deletes them ignores the failure —
  the signature of an `IO.delete`-style cleanup, not of a build step that would report.
- **Timing decides it.** Under `-Dsbt.log.level=debug` it does not happen. Same command, same cold state.
- **Compiling is never the trigger.** Cold + `compile` → 31. Cold + `Test/compile` → 31.
  `protocolZioGrpc/test` alone → 31. Only invocations that *run* server tests have ever lost them.
- **Not overlapping sbt processes.** No sbt server, no lingering `sbt-launch` between invocations.
- **It is flaky.** `sbt clean` then a server test run reproduced it twice, then passed five consecutive
  attempts (3× `testOnly`, 2× full `server/test`). Treat any claimed fix accordingly.

Relevant settings: `Test / fork = false`, `exportJars = false`, layering `ScalaLibrary` — so tests run in
sbt's own JVM and load classes lazily from the dependency's *classes directory*, through the cached
`ManagedClassLoader` that appears in the stack trace. That is why a mid-run deletion is fatal rather than
merely untidy.

## What was ruled out

- **Ordering.** `server.dependsOn(protocolZioGrpc)` already forces the dependency to compile first, and the
  log confirms it did. No ordering constraint can prevent a deletion that happens after everything compiled.
- **A downstream task eating it.** `server/Docker/publishLocal` leaves 31; `e2e/test` leaves 31 and passes.
- **"The build layout is unusual."** An earlier version of this note blamed `PB.protoSources` pointing at
  `modules/protocol`'s directory, and both modules generating into `sourceManaged/scalapb`. That was a
  hedge, not a finding: a shared proto directory consumed by several codegen modules is a supported
  sbt-protoc arrangement, and nothing in the investigation implicates it. Recorded so the next reader does
  not chase it.

## Not found: the actor

No deletion or invalidation line names the module in a debug log — because logging suppresses the race.
Identifying it needs filesystem tracing (`fs_usage`/`dtrace`, which want sudo) or an sbt run under a JVM
agent hooked on `File.delete`.

## What is at risk

1. **`tests.yml`** — cold, single invocation, exactly the shape that reproduces. Worst case is a red build,
   or *green with the module left empty*: locally, root `test` passed all 40 tests and ended with 0 class
   files. Nothing ships from this workflow.
2. **`release.yml`** — `sbt test` then `sbt publish`, separate invocations sharing a workspace. Publish
   recompiles and usually recovers, but `packageBin` reads the classes directory *after* compile, inside
   the same window. Landing there publishes a protos-only jar to a version GitHub Packages will never let
   you overwrite.
3. **The Docker image** — `Docker/stage` copies sibling projects' `packageBin` into `lib/`:
   `…/docker/stage/4/opt/docker/lib/…-protocol-zio-grpc-0.0.1-SNAPSHOT.jar`. Healthy it is 75,724 bytes;
   **earlier today that jar was 3,908 bytes — the two protos and nothing else.** An image built in that
   window starts and then dies at the first gRPC call. No CI job builds the image today; `sbt e2e` does,
   and a deploy workflow would.

Never observed in CI, and every historical run is green. That is not reassurance: runners are multi-core,
sbt parallelises, and a cold build that runs tests is exactly what CI does.

## Candidate fixes, none validated

- **`exportJars := true`** — the best fit. Dependencies reach the classpath as jars, and a jar is a
  snapshot: deleting loose class files afterwards cannot affect a run in progress, and `publish` packages
  from the same task. Removes the failure class rather than the trigger. Costs a packaging step per change.
- **`Test / fork := true`** — weaker than it looks. A forked JVM still reads classes lazily from the same
  directories; it only sidesteps sbt's cached classloader, which is not established as the culprit.
- **`concurrentRestrictions := Seq(Tags.limitAll(1))`** — stops it if the deleter is a parallel task, at a
  large cost in build time, aimed at a mechanism not yet identified.

**A guard that does not depend on the cause:** verify before publishing that the artifact contains classes —
`unzip -l …-protocol-zio-grpc*.jar | grep -q '\.class'` — and fail the release if it does not. Discussed,
deliberately not added yet.

## Where to pick it up

1. Try to get a reliable reproduction back: `sbt clean` in its own invocation, then `sbt server/test`,
   repeatedly, checking `find modules/protocol-zio-grpc/target -name '*.class' | wc -l` after each.
2. With a reproduction in hand, trace the deletion (`fs_usage -f filesys | grep unlink`, or a JVM agent on
   `File.delete`) — everything short of that has been guesswork.
3. Only then trial `exportJars`, and re-run the reproduction enough times to mean something.
