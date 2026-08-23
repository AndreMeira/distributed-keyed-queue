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
val nettyVersion          = "4.1.100.Final" // grpc-netty 1.64 is built against this; see the overrides below
val lettuceVersion        = "6.7.1.RELEASE"
val typesafeConfigVersion = "1.4.9"
val pureconfigVersion     = "0.17.10"
val chimneyVersion        = "1.10.0"
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
  "io.grpc"  % "grpc-netty"          % grpcVersion,
  "io.grpc"  % "grpc-core"           % grpcVersion,
  "io.grpc"  % "grpc-api"            % grpcVersion,
  "io.grpc"  % "grpc-stub"           % grpcVersion,
  "io.grpc"  % "grpc-protobuf"       % grpcVersion,
  "io.grpc"  % "grpc-util"           % grpcVersion,
  "io.netty" % "netty-buffer"                     % nettyVersion,
  "io.netty" % "netty-codec"                      % nettyVersion,
  "io.netty" % "netty-codec-http"                 % nettyVersion,
  "io.netty" % "netty-codec-http2"                % nettyVersion,
  "io.netty" % "netty-codec-socks"                % nettyVersion,
  "io.netty" % "netty-common"                     % nettyVersion,
  "io.netty" % "netty-handler"                    % nettyVersion,
  "io.netty" % "netty-handler-proxy"              % nettyVersion,
  "io.netty" % "netty-resolver"                   % nettyVersion,
  "io.netty" % "netty-codec-dns"                  % nettyVersion,
  "io.netty" % "netty-resolver-dns"               % nettyVersion,
  "io.netty" % "netty-transport"                  % nettyVersion,
  "io.netty" % "netty-transport-native-unix-common" % nettyVersion,
)


lazy val root = project
  .in(file("."))
  // JavaAppPackaging + DockerPlugin so `sbt Docker/publishLocal` produces the image the end-to-end suite
  // deploys. The service has no hand-written Dockerfile on purpose: the image a developer tests is then
  // the image sbt built, with no second definition of the entry point to keep in step.
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(
    name := "distributed-keyed-queue",
    Docker / packageName := "distributed-keyed-queue",
    // Pinned to a JRE newer than any JDK likely to build this: class files travel forward, not back.
    dockerBaseImage      := "eclipse-temurin:21-jre",
    dockerExposedPorts   := Seq(9000),
    // `:latest` as well as the version, so docker-compose.e2e.yml names an image that does not change
    // every time the version does.
    dockerUpdateLatest   := true,
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
      // Wire <-> domain mapping. The domain DTOs mirror the proto field for field precisely so these
      // transformers stay derivable: anything Chimney cannot derive is a mismatch worth looking at.
      "io.scalaland"                  %% "chimney"              % chimneyVersion,
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
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
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
  .dependsOn(root)
  .settings(
    name           := "distributed-keyed-queue-e2e",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio-test"     % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
    ),
    // The suite shells out to `docker compose`, and a forked JVM inherits the terminal's environment —
    // which is where DKQ_E2E_ENDPOINTS is read from when pointing the suite at an existing deployment.
    Test / fork     := true,
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )

// The whole thing: build the image, then run the suite against it.
addCommandAlias("e2e", ";root/Docker/publishLocal;e2e/test")
