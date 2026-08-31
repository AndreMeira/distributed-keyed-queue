// distributed-keyed-queue — a POC: a keyed queue, distributed over gRPC, with per-key serialisation held
// at the storage layer rather than in consumer memory.
// Design note: ../research/infrastructure/homelab-message-broker.md
//
// Built on homelab-toolkit-zio: `homelab-common` brings the messaging/flow/store ports (KeyedQueue,
// KeyLock, Distributer, PollConsumer), `homelab-postgres` the leased store behind them.

val scala3Version         = "3.8.3"
val zioVersion            = "2.1.23"
val zioPreludeVersion     = "1.0.0-RC47" // the toolkit's version; Validation is a domain-level type here
val toolkitVersion        = "0.0.1-alpha"
val scalapbVersion        = "0.11.17"       // keep in sync with compilerplugin in project/plugins.sbt
val zioGrpcVersion        = "0.6.3"
val grpcVersion           = "1.83.1"        // must match the grpc-core zio-grpc pulls, not scalapb's
val nettyVersion          = "4.1.100.Final" // grpc-netty 1.64 is built against this; see the overrides below
val lettuceVersion        = "6.7.1.RELEASE"
val typesafeConfigVersion = "1.4.9"
val pureconfigVersion     = "0.17.10"
val chimneyVersion        = "1.10.0"
val testcontainersVersion = "1.20.6"

ThisBuild / scalaVersion := scala3Version
ThisBuild / organization := "com.andremeira.homelab"
// Set by the release workflow from the tag, so a published version always corresponds to a commit CI built
// and tested. The default is what a laptop builds with, and is never pushed.
ThisBuild / version      := sys.env.getOrElse("RELEASE_VERSION", "0.0.1-SNAPSHOT")


ThisBuild / scalacOptions ++= Seq(
  "-Wvalue-discard",
  "-Wnonunit-statement",
  "-Wconf:msg=(unused.*value|discarded.*value|pure.*statement):error",
  "-Wconf:src=.*src_managed.*:silent", // generated ScalaPB code does not survive the flags above
)


// The toolkit lives in GitHub Packages; the resolver and the credential lookup are in
// project/GitHubPackages.scala so this file stays declarative. ThisBuild, so every module resolves it.
ThisBuild / resolvers += GitHubPackages.toolkit

ThisBuild / credentials ++= GitHubPackages.credentials


// The two contract modules are the only ones worth publishing: they are what a consumer of this service
// needs, and the modules that are not a contract opt out with `publish / skip` where they are defined.
ThisBuild / publishMavenStyle := true

ThisBuild / publishTo := Some(GitHubPackages.registry)


// One gRPC version and one Netty version across the whole build, including the end-to-end project.
//
// Both splits are invisible at compile time and appear only under real traffic. gRPC's is the one this
// build hit first: grpc-netty and grpc-core from different releases fail with a NoSuchMethodError deep in
// the transport.
//
// Netty's is worse, because it does not fail loudly. Lettuce pulls netty 4.1.118; grpc-netty 1.64 is built
// against 4.1.100 and reaches into Netty's HTTP/2 internals. Mix them and the *server* emits corrupt HPACK
// header blocks once several requests are in flight — the client reports `Incomplete header block fragment`,
// the server reports a truncated request, and neither is the culprit: one bad write desynchronises the
// connection in both directions. It reproduces through the plain Java blocking stub, so it is nothing to do
// with zio-grpc, and it never appears in a single-threaded test.
//
// **So netty is pinned to what grpc-netty was built against, not to the newest on the graph** — downgrading
// lettuce's netty rather than upgrading gRPC's. Lettuce uses stable Netty APIs and is happy on 4.1.100;
// grpc-netty is not happy on anything but its own. When bumping either, bump them *together*: read the
// grpc-java release notes for the Netty version it ships with, and set nettyVersion to exactly that.
//
// Found by the end-to-end suite (e2e/), which is the only thing here that puts real concurrent traffic on a
// connection. `WireSpec` in that project is the minimal reproduction, kept as a regression guard.
//
// ThisBuild, not per project: a second project resolving its own versions would reintroduce exactly this.
ThisBuild / dependencyOverrides ++= Seq(
  "io.grpc"  % "grpc-netty"                         % grpcVersion,
  "io.grpc"  % "grpc-core"                          % grpcVersion,
  "io.grpc"  % "grpc-api"                           % grpcVersion,
  "io.grpc"  % "grpc-stub"                          % grpcVersion,
  "io.grpc"  % "grpc-protobuf"                      % grpcVersion,
  "io.grpc"  % "grpc-util"                          % grpcVersion,
  "io.netty" % "netty-buffer"                       % nettyVersion,
  "io.netty" % "netty-codec"                        % nettyVersion,
  "io.netty" % "netty-codec-http"                   % nettyVersion,
  "io.netty" % "netty-codec-http2"                  % nettyVersion,
  "io.netty" % "netty-codec-socks"                  % nettyVersion,
  "io.netty" % "netty-common"                       % nettyVersion,
  "io.netty" % "netty-handler"                      % nettyVersion,
  "io.netty" % "netty-handler-proxy"                % nettyVersion,
  "io.netty" % "netty-resolver"                     % nettyVersion,
  "io.netty" % "netty-codec-dns"                    % nettyVersion,
  "io.netty" % "netty-resolver-dns"                 % nettyVersion,
  "io.netty" % "netty-transport"                    % nettyVersion,
  "io.netty" % "netty-transport-native-unix-common" % nettyVersion,
)


/**
 * The contract itself: every `.proto` in the repo lives here, and this module publishes the message types.
 *
 * '''One home for the protos, two artifacts generated from them.''' The service definition sits beside the
 * messages it uses — they are one contract, and splitting the *files* across modules named after Scala
 * concerns would hide that. What the module split is really about is what a consumer has to depend on: this
 * artifact carries `scalapb-runtime` and nothing else, so handling the data never drags in a transport or
 * an effect system.
 *
 * The service proto is excluded from generation here rather than moved away; it stays on protoc's include
 * path, which is how `protocolZioGrpc` resolves its import of the messages.
 */
lazy val protocol = project
  .in(file("modules/protocol"))
  .settings(
    name                                := "distributed-keyed-queue-protocol",
    // Everything under `protobuf` except the service: the messages are what this artifact is.
    Compile / PB.generate / excludeFilter := "*_service.proto",
    Compile / PB.targets := Seq(
      // Generator options live in the .proto (`option (scalapb.options)`), not here: zio-grpc's generator
      // reads them from the file, so setting flatPackage build-side desynchronises the two and the ZIO
      // stub ends up referring to a package ScalaPB no longer generates.
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    ),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion,
      // unpacks scalapb.proto onto protoc's include path, so the `option (scalapb.options)` in the
      // .proto files resolves
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf",
    ),
  )


/**
 * The ZIO-native stubs for the service, and nothing else.
 *
 * '''No sources of its own.''' It generates from `protocol`'s protos, taking the service definition and
 * leaving the messages — which stay on protoc's include path so the import resolves, and stay in
 * `protocol`'s jar so a consumer never has two copies of a message class on its classpath.
 *
 * This is what an RPC consumer depends on, and the end-to-end suite proves it is enough on its own — it
 * drives the deployment with this module and a transport, nothing else. It carries no transport itself,
 * because whether a consumer dials over netty, in-process or something else is the consumer's decision.
 */
lazy val protocolZioGrpc = project
  .in(file("modules/protocol-zio-grpc"))
  .dependsOn(protocol)
  .settings(
    name                                := "distributed-keyed-queue-protocol-zio-grpc",
    Compile / PB.protoSources           := Seq((protocol / Compile / sourceDirectory).value / "protobuf"),
    // The mirror of the exclusion in `protocol`: between them, every proto is generated exactly once.
    Compile / PB.generate / includeFilter := "*_service.proto",
    Compile / PB.targets       := Seq(
      scalapb.gen(grpc = true)          -> (Compile / sourceManaged).value / "scalapb",
      scalapb.zio_grpc.ZioCodeGenerator -> (Compile / sourceManaged).value / "scalapb",
    ),
    libraryDependencies ++= Seq(
      "dev.zio"                       %% "zio"                  % zioVersion,
      "com.thesamet.scalapb"          %% "scalapb-runtime-grpc" % scalapbVersion,
      "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-core"        % zioGrpcVersion,
      // unpacks scalapb.proto onto protoc's include path, so the `option (scalapb.options)` in the
      // .proto files resolves
      "com.thesamet.scalapb"          %% "scalapb-runtime"      % scalapbVersion % "protobuf",
    ),
  )


/**
 * The service: the domain, the Redis adapter, and the gRPC server that fronts them.
 *
 * This is the deployable — `sbt server/Docker/publishLocal` builds the image the compose stacks run.
 */
lazy val server = project
  .in(file("modules/server"))
  .dependsOn(protocolZioGrpc)
  // JavaAppPackaging + DockerPlugin so `sbt server/Docker/publishLocal` produces the image the end-to-end
  // suite deploys. The service has no hand-written Dockerfile on purpose: the image a developer tests is
  // then the image sbt built, with no second definition of the entry point to keep in step.
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(
    name                 := "distributed-keyed-queue-server",
    // The deployable ships as an image, not a jar — nothing should depend on it.
    publish / skip       := true,
    // Pinned rather than derived from `name`: the compose files and the cluster manifests name this image,
    // and they should not move because a module was renamed.
    Docker / packageName := "distributed-keyed-queue",
    // Pinned to a JRE newer than any JDK likely to build this: class files travel forward, not back.
    dockerBaseImage      := "eclipse-temurin:21-jre",
    dockerExposedPorts   := Seq(9000),
    // `:latest` as well as the version, so docker-compose.e2e.yml names an image that does not change
    // every time the version does.
    dockerUpdateLatest   := true,
    libraryDependencies ++= Seq(
      // the toolkit: ports + the Postgres adapter behind them (magnum, Hikari, Flyway come transitively)
      "com.andremeira.homelab" %% "homelab-common"   % toolkitVersion,
      "com.andremeira.homelab" %% "homelab-postgres" % toolkitVersion,
      "dev.zio"                %% "zio"              % zioVersion,
      // Validation as a value: accumulating rather than fail-fast, which ZIO's error channel cannot be.
      // Comes transitively with homelab-common; named here because the domain uses it directly.
      "dev.zio"                %% "zio-prelude"      % zioPreludeVersion,
      // Redis: the substrate. Lettuce rather than zio-redis, which derives its Input/Output from a
      // zio-schema BinaryCodec — that encodes keys and args, and this design needs them byte-exact
      // (the Lua builds key names by concatenation) plus heterogeneous script replies.
      "io.lettuce"              % "lettuce-core"     % lettuceVersion,
      // Config: HOCON under resources/config, read with pureconfig — the homelab's convention, and the
      // one that lets a file carry both a working default and an env override for the same key.
      "com.typesafe"            % "config"           % typesafeConfigVersion,
      "com.github.pureconfig"  %% "pureconfig-core"  % pureconfigVersion,
      // Wire <-> domain mapping. The domain DTOs mirror the proto field for field precisely so these
      // transformers stay derivable: anything Chimney cannot derive is a mismatch worth looking at.
      "io.scalaland"           %% "chimney"          % chimneyVersion,
      // The transport the server listens on; the generated stubs and their runtime come from `protocol`.
      "io.grpc"                 % "grpc-netty"       % grpcVersion,
      "dev.zio"                %% "zio-test"         % zioVersion            % Test,
      "dev.zio"                %% "zio-test-sbt"     % zioVersion            % Test,
      "org.testcontainers"      % "testcontainers"   % testcontainersVersion % Test,
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )


/**
 * The umbrella: builds nothing of its own, fans `compile` and `test` out to the modules.
 *
 * `e2e` is deliberately absent from the fan-out — it needs Docker and an image, and `sbt test` must stay
 * something worth running on every save. Run it with `sbt e2e`.
 */
lazy val root = project
  .in(file("."))
  .aggregate(protocol, protocolZioGrpc, server)
  .settings(
    name           := "distributed-keyed-queue",
    publish / skip := true,
  )


/**
 * The end-to-end suite: several real instances in Docker, one Valkey, clients over the wire.
 *
 * A separate project rather than another test source folder, because these tests cost minutes and need
 * Docker — `sbt test` must stay something worth running on every save. Run them with `sbt e2e` (which
 * builds the image first) or `sbt e2e/test` against a stack you already have up.
 */
lazy val e2e = project
  .in(file("e2e"))
  // the contract, not `server`: these tests are a *consumer*, and depending on the contract alone is the
  // property worth keeping — nothing here can reach past the wire into the implementation it is testing.
  .dependsOn(protocolZioGrpc)
  .settings(
    name           := "distributed-keyed-queue-e2e",
    publish / skip := true,
    libraryDependencies ++= Seq(
      // A consumer picks its own transport; the contract does not ship one.
      "io.grpc"  % "grpc-netty"   % grpcVersion,
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
    ),
    // The suite shells out to `docker compose`, and a forked JVM inherits the terminal's environment —
    // which is where DKQ_E2E_ENDPOINTS is read from when pointing the suite at an existing deployment.
    Test / fork    := true,
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )


// The whole thing: build the image, then run the suite against it.
addCommandAlias("e2e", ";server/Docker/publishLocal;e2e/test")
