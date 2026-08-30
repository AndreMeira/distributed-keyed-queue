---
title: "One contract, two artifacts — how the protobuf build is wired"
type: learning-material
status: current
updated: 2026-08-30
tags: [protobuf, scalapb, sbt-protoc, zio-grpc, build, codegen]
---

# One contract, two artifacts

Both `.proto` files live in one directory, and two modules generate from it: `protocol` publishes the
message types, `protocolZioGrpc` publishes the ZIO stubs. No message class is generated twice, and no
consumer of the data inherits gRPC or ZIO.

This note is about the *build* — the mechanism, and the parts of it that are not obvious from reading
`build.sbt`.

## The mechanism: `protoSources` has two jobs

Everything here follows from one fact about sbt-protoc, and it is worth stating exactly because the whole
arrangement rests on it:

**A directory in `PB.protoSources` is both the place files to generate are looked for, *and* a directory on
protoc's include path — but only the first of those is filtered.**

In the plugin's own terms:

```scala
// which files get generated: protoSources, narrowed by the filters
(srcDir ** (toInclude -- toExclude)).get

// what protoc can resolve imports against: protoSources, unfiltered
PB.includePaths := PB.includePaths ++ PB.protoSources ++ Seq(externalIncludePath, …)
```

So a proto can be *visible* to a module without being *generated* by it. That is the whole trick.

## What that buys

```scala
lazy val protocol = project
  .settings(
    Compile / PB.generate / excludeFilter := "*_service.proto",
    Compile / PB.targets := Seq(scalapb.gen() -> …),
  )

lazy val protocolZioGrpc = project
  .dependsOn(protocol)
  .settings(
    Compile / PB.protoSources            := Seq((protocol / Compile / sourceDirectory).value / "protobuf"),
    Compile / PB.generate / includeFilter := "*_service.proto",
    Compile / PB.targets := Seq(scalapb.gen(grpc = true) -> …, scalapb.zio_grpc.ZioCodeGenerator -> …),
  )
```

The two filters are mirror images, so between them **every proto is generated exactly once**. The
zio-grpc module sees `keyed_queue.proto` — it has to, since the service file imports it — but does not
generate it, so the message classes exist in one jar only.

**Why that matters:** generate the messages in both and every consumer of the RPC stubs has two copies of
`Message` on its classpath, from two jars, with no error until something fails to cast at runtime.

## The parts that surprise

**`PB.protoSources` defaults with `+=`, not `:=`.** The plugin adds `sourceDirectory / "protobuf"`
automatically. `protocolZioGrpc` uses `:=` deliberately, because it has no sources of its own — it is build
configuration and generated output, nothing else. Using `+=` there would silently add an empty directory.

**Generator options live in the `.proto`, not in `build.sbt`.** `option (scalapb.options) = { flat_package:
true }` is in the file because zio-grpc's generator reads it from there. Set it build-side instead and the
two generators disagree: ScalaPB emits one package, the ZIO stub refers to another.

**A `% "protobuf"`-scoped dependency is not a library dependency.** `scalapb-runtime % "protobuf"` exists to
unpack `scalapb/scalapb.proto` onto protoc's include path so `import "scalapb/scalapb.proto"` resolves. It
contributes nothing to the classpath, and both modules need their own.

**The generated stub object is named after the *file*, not the service.** `keyed_queue_service.proto`
produces `ZioKeyedQueueService`; renaming the file renames the Scala object every consumer imports. Worth
knowing before splitting or renaming a proto — it is a source-breaking change that no proto-level review
would flag.

## What replaced what

An earlier version put the service proto in the zio-grpc module and reached back for the messages:

```scala
Compile / PB.protocOptions += "--proto_path=" + …protocol…/"protobuf"
```

That worked, but it hand-rolled what `protoSources` already does, and it left the two halves of one contract
in directories named after Scala concerns. Pointing `protoSources` at the one directory and filtering does
the same job with the plugin's own machinery, and puts the service definition beside the messages it uses.

**`homelab-schemas` solves this differently**, with two top-level directories — `proto/` for messages,
`proto-service/` for services — and no filters. That is the better shape when protos are the repo's whole
purpose and nothing else lives there. Filters suit a service repo, where the protos are one part of
something larger and a top-level directory per generator flavour would be noise.

## Checking it is still right

Three commands, worth running after any change to the proto layout:

```bash
# 1. Each module generates its own half, and nothing else.
ls modules/protocol/target/scala-*/src_managed/main/scalapb/homelab/keyedqueue/v1/
ls modules/protocol-zio-grpc/target/scala-*/src_managed/main/scalapb/homelab/keyedqueue/v1/

# 2. No class is in both jars.
RELEASE_VERSION=0.0.0 sbt 'protocol/publishLocal' 'protocolZioGrpc/publishLocal'
unzip -l ~/.ivy2/local/.../distributed-keyed-queue-protocol-*_3/0.0.0/jars/*.jar | grep keyedqueue/v1

# 3. The wire artifact still has no ZIO and no gRPC.
grep artifactId ~/.ivy2/local/.../distributed-keyed-queue-protocol_3/0.0.0/poms/*.pom
```

The third is the one that silently regresses: adding a generator or a dependency to `protocol` to fix
something local is easy, and nothing fails — the artifact just quietly starts dragging an effect system into
every consumer that only wanted the data.
