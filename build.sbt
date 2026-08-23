// distributed-keyed-queue — a POC: a keyed queue, distributed over gRPC, with per-key serialisation held
// at the storage layer rather than in consumer memory.
// Design note: ../research/infrastructure/homelab-message-broker.md
//
// Built on homelab-toolkit-zio: `homelab-common` brings the messaging/flow/store ports (KeyedQueue,
// KeyLock, Distributer, PollConsumer), `homelab-postgres` the leased store behind them.

val scala3Version         = "3.8.3"
val zioVersion            = "2.1.23"
val toolkitVersion        = "0.0.1-alpha"
val scalapbVersion        = "0.11.17" // keep in sync with compilerplugin in project/plugins.sbt
val zioGrpcVersion        = "0.6.3"
val grpcVersion           = "1.64.0" // must match the grpc-core zio-grpc pulls, not scalapb's
val lettuceVersion        = "6.7.1.RELEASE"
val typesafeConfigVersion = "1.4.9"
val pureconfigVersion     = "0.17.10"
val testcontainersVersion = "1.20.6"

ThisBuild / scalaVersion := scala3Version
ThisBuild / organization := "com.andremeira.homelab"
ThisBuild / version      := "0.0.1-SNAPSHOT"

ThisBuild / scalacOptions ++= Seq(
  "-Wvalue-discard",
  "-Wnonunit-statement",
  "-Wconf:msg=(unused.*value|discarded.*value|pure.*statement):error",
  "-Wconf:src=.*src_managed.*:silent", // generated ScalaPB code does not survive the flags above
)


// The toolkit lives in GitHub Packages; the resolver and the credential lookup are in
// project/GitHubPackages.scala so this file stays declarative.
resolvers += GitHubPackages.toolkit

ThisBuild / credentials ++= GitHubPackages.credentials


lazy val root = project
  .in(file("."))
  .settings(
    name := "distributed-keyed-queue",
    Compile / PB.targets := Seq(
      // Generator options live in the .proto (`option (scalapb.options)`), not here: zio-grpc's generator
      // reads them from the file, so setting flatPackage build-side desynchronises the two and the ZIO
      // stub ends up referring to a package ScalaPB no longer generates.
      scalapb.gen(grpc = true)          -> (Compile / sourceManaged).value / "scalapb",
      scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value / "scalapb",
    ),
    libraryDependencies ++= Seq(
      // the toolkit: ports + the Postgres adapter behind them (magnum, Hikari, Flyway come transitively)
      "com.andremeira.homelab"        %% "homelab-common"       % toolkitVersion,
      "com.andremeira.homelab"        %% "homelab-postgres"     % toolkitVersion,
      "dev.zio"                       %% "zio"                  % zioVersion,
      // Redis: the substrate. Lettuce rather than zio-redis, which derives its Input/Output from a
      // zio-schema BinaryCodec — that encodes keys and args, and this design needs them byte-exact
      // (the Lua builds key names by concatenation) plus heterogeneous script replies.
      "io.lettuce"                     % "lettuce-core"         % lettuceVersion,
      // Config: HOCON under resources/config, read with pureconfig — the homelab's convention, and the
      // one that lets a file carry both a working default and an env override for the same key.
      "com.typesafe"                   % "config"               % typesafeConfigVersion,
      "com.github.pureconfig"         %% "pureconfig-core"      % pureconfigVersion,
      // gRPC: generated stubs need the runtime; the server needs a transport (schemas ships neither)
      "com.thesamet.scalapb"          %% "scalapb-runtime-grpc" % scalapbVersion,
      "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-core"        % zioGrpcVersion,
      // NOT scalapb.compiler.Version.grpcJavaVersion: that is 1.62.2 here, while zio-grpc-core pulls
      // grpc-core 1.64.0, and the pair fails at runtime with a NoSuchMethodError deep in the transport.
      // The transport must match the core, so it is pinned to the same version and overridden below.
      "io.grpc"                        % "grpc-netty"           % grpcVersion,
      "dev.zio"                       %% "zio-test"             % zioVersion            % Test,
      "dev.zio"                       %% "zio-test-sbt"         % zioVersion            % Test,
      "org.testcontainers"             % "testcontainers"       % testcontainersVersion % Test,
    ),
    // unpacks scalapb.proto onto protoc's include path, for the package-remap option
    libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf",
    // One gRPC version across the graph: a split between transport and core is only visible at runtime.
    dependencyOverrides ++= Seq(
      "io.grpc" % "grpc-netty"          % grpcVersion,
      "io.grpc" % "grpc-core"           % grpcVersion,
      "io.grpc" % "grpc-api"            % grpcVersion,
      "io.grpc" % "grpc-stub"           % grpcVersion,
      "io.grpc" % "grpc-protobuf"       % grpcVersion,
      "io.grpc" % "grpc-util"           % grpcVersion,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )
