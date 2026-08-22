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


// The toolkit lives in GitHub Packages, which serves Maven only to authenticated callers — public repo or
// not. Credentials come from the environment in CI and from ~/.sbt/1.0/credentials on a laptop; sbt does
// not read that file unless a build asks it to, which is what the helper below does.
resolvers += "homelab-toolkit-zio" at "https://maven.pkg.github.com/AndreMeira/homelab-toolkit-zio"

ThisBuild / credentials ++= githubCredentials

def githubCredentials: Seq[Credentials] =
  (sys.env.get("GITHUB_ACTOR"), sys.env.get("GITHUB_TOKEN")) match {
    case (Some(actor), Some(token)) =>
      Seq(Credentials("GitHub Package Registry", "maven.pkg.github.com", actor, token))
    case _ =>
      val local = Path.userHome / ".sbt" / "1.0" / "credentials"
      if (local.exists) Seq(Credentials(local)) else Nil
  }


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
      // gRPC: generated stubs need the runtime; the server needs a transport (schemas ships neither)
      "com.thesamet.scalapb"          %% "scalapb-runtime-grpc" % scalapbVersion,
      "com.thesamet.scalapb.zio-grpc" %% "zio-grpc-core"        % zioGrpcVersion,
      "io.grpc"                        % "grpc-netty"           % scalapb.compiler.Version.grpcJavaVersion,
      "dev.zio"                       %% "zio-test"             % zioVersion            % Test,
      "dev.zio"                       %% "zio-test-sbt"         % zioVersion            % Test,
      "org.testcontainers"             % "postgresql"           % testcontainersVersion % Test,
    ),
    // unpacks scalapb.proto onto protoc's include path, for the package-remap option
    libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf",
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
  )
